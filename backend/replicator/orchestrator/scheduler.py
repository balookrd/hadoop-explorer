"""Планировщик периодических задач репликации (Cron Scheduler).

Позволяет настраивать расписание выполнения репликации
и автоматически перезапускать задачи по таймеру / cron.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import logging
import re
from typing import Optional

import uuid
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.replicator.orchestrator.models import JobModel, JobRunModel

logger = logging.getLogger("replicator.scheduler")


def prune_job_runs(db: Session, job_id: str, retention_limit: int = 20) -> int:
    """Очищает старые записи истории запусков для задачи сверх лимита retention_limit."""
    if retention_limit <= 0:
        return 0
    runs = db.query(JobRunModel).filter(JobRunModel.job_id == job_id).order_by(JobRunModel.run_number.desc()).all()
    deleted_count = 0
    if len(runs) > retention_limit:
        for old_run in runs[retention_limit:]:
            db.delete(old_run)
            deleted_count += 1
    return deleted_count


def compute_next_run(cron_expr: str, base_time: Optional[datetime] = None) -> datetime:
    """
    Рассчитывает время следующего запуска задачи на основе cron-выражения или пресета.
    """
    now = base_time or datetime.now(timezone.utc)
    expr = (cron_expr or "").strip().lower()

    if expr in ("@minutely", "* * * * *"):
        return now + timedelta(minutes=1)
    elif expr in ("@every_5m", "*/5 * * * *"):
        return now + timedelta(minutes=5)
    elif expr in ("@every_15m", "*/15 * * * *"):
        return now + timedelta(minutes=15)
    elif expr in ("@hourly", "0 * * * *"):
        return now + timedelta(hours=1)
    elif expr in ("@daily", "0 0 * * *", "@midnight"):
        return now + timedelta(days=1)
    elif expr.startswith("every_") and expr.endswith("s"):
        try:
            sec = int(expr.replace("every_", "").replace("s", ""))
            return now + timedelta(seconds=max(5, sec))
        except ValueError:
            pass

    # Разбор интервалов вида */N * * * * (каждые N минут)
    m = re.match(r"^\*/(\d+)\s+\*\s+\*\s+\*\s+\*$", expr)
    if m:
        mins = int(m.group(1))
        return now + timedelta(minutes=max(1, mins))

    # По умолчанию через 1 час, если выражение не распознано
    return now + timedelta(hours=1)


async def scheduler_tick(db: Session, now: datetime) -> int:
    """
    Один шаг проверки задач по расписанию.
    Находит запланированные задачи, время запуска которых наступило, создает запись в истории и ставит их в очередь.
    """
    triggered_count = 0
    scheduled_jobs = db.query(JobModel).filter(JobModel.is_scheduled == True).all()

    for job in scheduled_jobs:
        if job.next_run_at is None:
            job.next_run_at = compute_next_run(job.cron_expression or "@hourly", now)
            if job.status not in ("RUNNING", "QUEUED"):
                job.status = "SCHEDULED"
            continue

        # Приводим к timezone-aware при необходимости
        next_run = job.next_run_at
        if next_run.tzinfo is None:
            next_run = next_run.replace(tzinfo=timezone.utc)

        if now >= next_run:
            # Запускаем только если задача не выполняется прямо сейчас
            if job.status in ("SCHEDULED", "COMPLETED", "FAILED", "CANCELLED", "QUEUED"):
                next_run_num = (
                    db.query(func.max(JobRunModel.run_number)).filter(JobRunModel.job_id == job.id).scalar() or 0
                ) + 1

                run_id = str(uuid.uuid4())
                run = JobRunModel(
                    id=run_id,
                    job_id=job.id,
                    run_number=next_run_num,
                    trigger_type="SCHEDULED",
                    status="QUEUED",
                    total_bytes=job.total_bytes,
                    copied_bytes=0,
                    started_at=None,
                    completed_at=None,
                    duration_seconds=None,
                    average_speed_mb_s=None,
                    error_message=None,
                    message=f"Запуск #{next_run_num} по расписанию ({job.cron_expression})",
                    triggered_by="scheduler",
                    created_at=now,
                    updated_at=now,
                )
                db.add(run)
                db.flush()

                logger.info(
                    f"Шедулер запускает запуск #{next_run_num} для задачи {job.id} по расписанию "
                    f"('{job.cron_expression}'). Источник: {job.source_path}"
                )
                job.active_run_id = run_id
                job.status = "QUEUED"
                job.copied_bytes = 0
                job.started_at = None
                job.completed_at = None
                job.last_run_at = now
                job.next_run_at = compute_next_run(job.cron_expression or "@hourly", now)
                job.message = f"Запуск #{next_run_num} по расписанию ({now.strftime('%H:%M:%S UTC')})"
                prune_job_runs(db, job.id, job.history_retention_runs)
                triggered_count += 1
            else:
                # Если задача еще RUNNING, сдвигаем следующий запуск
                job.next_run_at = compute_next_run(job.cron_expression or "@hourly", now)
        else:
            # Пока время запуска не наступило, если задача не в очереди и не работает — статус SCHEDULED
            if job.status not in ("RUNNING", "QUEUED") and job.status != "SCHEDULED":
                job.status = "SCHEDULED"

    if triggered_count > 0:
        db.commit()

    return triggered_count


async def run_scheduler_daemon(session_factory, poll_interval_sec: float = 3.0):
    """Фоновый асинхронный демон планировщика задач."""
    logger.info("Запущен планировщик задач репликации (Scheduler Daemon)")
    while True:
        try:
            now = datetime.now(timezone.utc)
            db: Session = session_factory()
            try:
                await scheduler_tick(db, now)
            finally:
                db.close()
        except asyncio.CancelledError:
            logger.info("Остановка планировщика задач...")
            break
        except Exception as e:
            logger.error(f"Ошибка в цикле планировщика: {e}")

        await asyncio.sleep(poll_interval_sec)
