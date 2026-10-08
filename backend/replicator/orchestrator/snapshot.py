"""Модуль парсинга HDFS Snapshot Diff и генерации подзадач инкрементальной репликации."""

from dataclasses import dataclass, field
import logging
import posixpath
import re
from typing import List, Optional, Tuple
import uuid

from sqlalchemy.orm import Session

from backend.replicator.orchestrator.models import JobModel, TaskModel

logger = logging.getLogger("replicator.snapshot")


@dataclass
class SnapshotDiffResult:
    """Результат парсинга различий между HDFS снимками."""

    added: List[str] = field(default_factory=list)
    modified: List[str] = field(default_factory=list)
    deleted: List[str] = field(default_factory=list)
    renamed: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def total_changed_files(self) -> int:
        return len(self.added) + len(self.modified) + len(self.deleted) + len(self.renamed)


def clean_relative_path(path_str: str) -> str:
    """Удаляет ведущие './' и пробелы из относительного пути."""
    p = path_str.strip()
    if p.startswith("./"):
        p = p[2:]
    return p.lstrip("/")


def parse_snapshot_diff(diff_output: str, base_path: str = "") -> SnapshotDiffResult:
    """
    Парсит вывод консольной команды:
    `hdfs dfs -snapshotDiff <path> <snap1> <snap2>`

    Формат строк:
    +   ./path/new_file       (Added / Created)
    -   ./path/deleted_file   (Deleted)
    M   ./path/modified_file  (Modified)
    R   ./old_name -> ./new_name (Renamed)
    """
    result = SnapshotDiffResult()
    if not diff_output:
        return result

    base = base_path.rstrip("/") if base_path else ""

    for line in diff_output.strip().splitlines():
        line = line.strip()
        # Пропуск строк заголовков и пустых строк
        if not line or line.startswith("Difference between snapshot"):
            continue

        parts = line.split(maxsplit=1)
        if len(parts) < 2:
            continue

        op_code, entry_path = parts[0], parts[1].strip()

        # Обработка переименований (R   path1 -> path2)
        if op_code == "R" and "->" in entry_path:
            old_p, new_p = entry_path.split("->", 1)
            old_clean = clean_relative_path(old_p)
            new_clean = clean_relative_path(new_p)

            old_full = posixpath.join(base, old_clean) if base else old_clean
            new_full = posixpath.join(base, new_clean) if base else new_clean
            result.renamed.append((old_full, new_full))
            # При переименовании целевой файл также считается добавленным/измененным для репликации
            result.added.append(new_full)
            result.deleted.append(old_full)
            continue

        clean_path = clean_relative_path(entry_path)
        full_path = posixpath.join(base, clean_path) if base else clean_path

        if op_code == "+":
            result.added.append(full_path)
        elif op_code == "M":
            result.modified.append(full_path)
        elif op_code == "-":
            result.deleted.append(full_path)
        else:
            logger.debug(f"Неизвестный код операции в snapshotDiff: {op_code} ({line})")

    logger.info(
        f"Snapshot Diff обработан: добавлено={len(result.added)}, "
        f"изменено={len(result.modified)}, удалено={len(result.deleted)}, переименовано={len(result.renamed)}"
    )
    return result


def generate_tasks_from_snapshot(
    db: Session,
    job: JobModel,
    diff_result: SnapshotDiffResult,
    source_base: Optional[str] = None,
    target_base: Optional[str] = None,
) -> List[TaskModel]:
    """
    Генерирует отдельные подзадачи TaskModel в БД для каждого файла из списков added и modified.

    Поддерживает:
    - Наследование параметров сервисной техучетки от родительской задачи JobModel
    - Сопоставление относительных путей источника (source_base) и приемника (target_base)
    - Атомарное сохранение транзакции в базе данных
    """
    src_base = (source_base or job.source_path).rstrip("/")
    dst_base = (target_base or job.target_path).rstrip("/")

    tasks: List[TaskModel] = []

    # 1. Задачи для добавленных файлов (ADD)
    for file_path in diff_result.added:
        # Вычисляем относительный путь от корня репликации
        if file_path.startswith(src_base):
            rel_path = file_path[len(src_base) :].lstrip("/")
        else:
            rel_path = clean_relative_path(file_path)

        src_full = posixpath.join(src_base, rel_path)
        dst_full = posixpath.join(dst_base, rel_path)

        task = TaskModel(
            id=str(uuid.uuid4()),
            job_id=job.id,
            source_path=src_full,
            target_path=dst_full,
            action_type="ADD",
            status="QUEUED",
            total_bytes=0,
            copied_bytes=0,
            run_as_service_account=job.run_as_service_account,
            execution_principal=job.execution_principal,
            message="Задача на репликацию добавленного файла (Snapshot Diff)",
        )
        tasks.append(task)

    # 2. Задачи для измененных файлов (MODIFY)
    for file_path in diff_result.modified:
        if file_path.startswith(src_base):
            rel_path = file_path[len(src_base) :].lstrip("/")
        else:
            rel_path = clean_relative_path(file_path)

        src_full = posixpath.join(src_base, rel_path)
        dst_full = posixpath.join(dst_base, rel_path)

        task = TaskModel(
            id=str(uuid.uuid4()),
            job_id=job.id,
            source_path=src_full,
            target_path=dst_full,
            action_type="MODIFY",
            status="QUEUED",
            total_bytes=0,
            copied_bytes=0,
            run_as_service_account=job.run_as_service_account,
            execution_principal=job.execution_principal,
            message="Задача на репликацию измененного файла (Snapshot Diff)",
        )
        tasks.append(task)

    # 3. Задачи на удаление (DELETE)
    for file_path in diff_result.deleted:
        if file_path.startswith(src_base):
            rel_path = file_path[len(src_base) :].lstrip("/")
        else:
            rel_path = clean_relative_path(file_path)

        src_full = posixpath.join(src_base, rel_path)
        dst_full = posixpath.join(dst_base, rel_path)

        task = TaskModel(
            id=str(uuid.uuid4()),
            job_id=job.id,
            source_path=src_full,
            target_path=dst_full,
            action_type="DELETE",
            status="QUEUED",
            total_bytes=0,
            copied_bytes=0,
            run_as_service_account=job.run_as_service_account,
            execution_principal=job.execution_principal,
            message="Задача на удаление файла в приемнике (Snapshot Diff)",
        )
        tasks.append(task)

    if tasks:
        db.add_all(tasks)
        job.message = f"Сгенерировано {len(tasks)} задач из Snapshot Diff"
        db.commit()
        for t in tasks:
            db.refresh(t)

    logger.info(f"Для задачи {job.id} успешно создано {len(tasks)} подзадач в БД")
    return tasks
