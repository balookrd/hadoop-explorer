import csv
import io
import json
import logging
from typing import Optional, List, Dict, Any
from app.models.hdfs import FilePreviewResponse

logger = logging.getLogger(__name__)


def format_size(size_bytes: int) -> str:
    """Форматирует размер в байтах в человекочитаемый вид."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


class PreviewService:
    @staticmethod
    def generate_preview(path: str, cluster_id: str, content: bytes, total_size: int, max_bytes: int) -> FilePreviewResponse:
        truncated = total_size > max_bytes
        file_name = path.split("/")[-1].lower()

        # 1. CSV / TSV
        if file_name.endswith(".csv") or file_name.endswith(".tsv"):
            try:
                delimiter = "\t" if file_name.endswith(".tsv") else ","
                text_stream = io.StringIO(content.decode("utf-8", errors="replace"))
                reader = csv.reader(text_stream, delimiter=delimiter)
                rows = []
                columns = []
                for i, row in enumerate(reader):
                    if i == 0:
                        columns = row
                    else:
                        rows.append(row)
                    if len(rows) >= 100:  # максимум 100 строк для превью
                        break

                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="csv",
                    size=total_size,
                    truncated=truncated,
                    columns=columns,
                    rows=rows,
                    row_count=len(rows)
                )
            except Exception as e:
                logger.warning(f"Не удалось распарсить CSV {path}: {e}")

        # 2. JSON
        if file_name.endswith(".json") or file_name.endswith(".jsonl"):
            try:
                text = content.decode("utf-8", errors="replace")
                if file_name.endswith(".json"):
                    parsed = json.loads(text)
                    formatted = json.dumps(parsed, indent=2, ensure_ascii=False)
                else:
                    formatted = text  # json lines оставляем построчно
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="json",
                    size=total_size,
                    truncated=truncated,
                    content=formatted
                )
            except Exception:
                pass  # Fallback to plain text

        # 3. Parquet (Apache Parquet)
        if file_name.endswith(".parquet") or file_name.endswith(".parq"):
            size_str = format_size(total_size)
            max_str = format_size(max_bytes)

            # Специфика Parquet: метаданные схемы и footer находятся в самом конце файла.
            # Если файл превышает max_bytes, footer усечен и файл невозможно распарсить через pyarrow.
            if truncated:
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="parquet",
                    size=total_size,
                    truncated=True,
                    content=(
                        f"Файл Apache Parquet (размер: {size_str}) превышает лимит предварительного просмотра ({max_str}). "
                        "Футер с метаданными схемы усечен. Скачайте файл для полного анализа или выполнения SQL-запросов."
                    ),
                    columns=[],
                    rows=[],
                    row_count=0
                )

            try:
                import pyarrow.parquet as pq
                reader = pq.ParquetFile(io.BytesIO(content))
                table = reader.read_row_group(0) if reader.num_row_groups > 0 else reader.read()
                columns = list(table.column_names)
                pylist = table.slice(0, 50).to_pylist()
                safe_rows = [[str(r.get(c, "")) if r.get(c) is not None else "" for c in columns] for r in pylist]
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="parquet",
                    size=total_size,
                    truncated=truncated,
                    columns=columns,
                    rows=safe_rows,
                    row_count=len(safe_rows)
                )
            except ImportError:
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="parquet",
                    size=total_size,
                    truncated=truncated,
                    content="Файл формата Apache Parquet. Для табличного предпросмотра на сервере требуется модуль pyarrow. Скачайте файл для локального анализа.",
                    columns=[],
                    rows=[],
                    row_count=0
                )
            except Exception as e:
                logger.warning(f"Ошибка парсинга Parquet файла {path}: {e}")
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="parquet",
                    size=total_size,
                    truncated=truncated,
                    content=f"Не удалось прочитать структуру Parquet файла ({e}). Скачайте файл для локального анализа.",
                    columns=[],
                    rows=[],
                    row_count=0
                )

        # 4. ORC (Optimized Row Columnar)
        if file_name.endswith(".orc"):
            size_str = format_size(total_size)
            max_str = format_size(max_bytes)

            # Специфика ORC: PostScript и футер с метаданными также находятся в конце файла.
            if truncated:
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="orc",
                    size=total_size,
                    truncated=True,
                    content=(
                        f"Файл Apache ORC (размер: {size_str}) превышает лимит предварительного просмотра ({max_str}). "
                        "Футер с метаданными схемы усечен. Скачайте файл для полного анализа или выполнения SQL-запросов."
                    ),
                    columns=[],
                    rows=[],
                    row_count=0
                )

            try:
                import pyarrow.orc as orc
                reader = orc.ORCFile(io.BytesIO(content))
                table = reader.read()
                columns = list(table.column_names)
                pylist = table.slice(0, 50).to_pylist()
                safe_rows = [[str(r.get(c, "")) if r.get(c) is not None else "" for c in columns] for r in pylist]
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="orc",
                    size=total_size,
                    truncated=truncated,
                    columns=columns,
                    rows=safe_rows,
                    row_count=len(safe_rows)
                )
            except ImportError:
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="orc",
                    size=total_size,
                    truncated=truncated,
                    content="Файл формата Apache ORC. Для табличного предпросмотра на сервере требуется модуль pyarrow.orc. Скачайте файл для локального анализа.",
                    columns=[],
                    rows=[],
                    row_count=0
                )
            except Exception as e:
                logger.warning(f"Ошибка парсинга ORC файла {path}: {e}")
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="orc",
                    size=total_size,
                    truncated=truncated,
                    content=f"Не удалось прочитать структуру ORC файла ({e}). Скачайте файл для локального анализа.",
                    columns=[],
                    rows=[],
                    row_count=0
                )

        # 5. Текстовые файлы (txt, log, yaml, yml, conf, xml, properties, sql, sh, py, etc.)
        try:
            text = content.decode("utf-8")
            return FilePreviewResponse(
                cluster_id=cluster_id,
                path=path,
                file_type="text",
                size=total_size,
                truncated=truncated,
                content=text
            )
        except UnicodeDecodeError:
            # Проверяем, может это частично читаемый текст
            text_lossy = content.decode("utf-8", errors="replace")
            # Если доля непечатаемых символов мала, показываем текст, иначе binary
            non_printable = sum(1 for c in text_lossy[:500] if ord(c) < 32 and c not in "\n\r\t")
            if non_printable > 50:
                return FilePreviewResponse(
                    cluster_id=cluster_id,
                    path=path,
                    file_type="binary",
                    size=total_size,
                    truncated=truncated,
                    content="Бинарный файл. Предпросмотр недоступен. Используйте кнопку 'Скачать'."
                )
            return FilePreviewResponse(
                cluster_id=cluster_id,
                path=path,
                file_type="text",
                size=total_size,
                truncated=truncated,
                content=text_lossy
            )


preview_service = PreviewService()
