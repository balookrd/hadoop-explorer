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


class _SeekableFooterStream(io.RawIOBase):
    """Потоковый виртуальный адаптер для чтения метаданных футера больших файлов."""

    def __init__(self, total_size: int, footer: bytes, header: bytes = b""):
        self.total_size = total_size
        self.footer = footer
        self.header = header
        self.footer_offset = max(0, total_size - len(footer))
        self.pos = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self.pos = offset
        elif whence == 1:
            self.pos += offset
        elif whence == 2:
            self.pos = self.total_size + offset
        return self.pos

    def tell(self) -> int:
        return self.pos

    def readinto(self, b) -> int:
        buf_len = len(b)
        if self.pos >= self.total_size:
            return 0
        if self.pos < len(self.header):
            chunk = self.header[self.pos : self.pos + buf_len]
        elif self.pos >= self.footer_offset:
            idx = self.pos - self.footer_offset
            chunk = self.footer[idx : idx + buf_len]
        else:
            chunk = b"\x00" * min(buf_len, self.footer_offset - self.pos)
        b[: len(chunk)] = chunk
        self.pos += len(chunk)
        return len(chunk)


class PreviewService:
    @staticmethod
    def generate_preview(
        path: str,
        cluster_id: str,
        content: bytes,
        total_size: int,
        max_bytes: int,
        footer_bytes: Optional[bytes] = None,
    ) -> FilePreviewResponse:
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
                    row_count=len(rows),
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
                    content=formatted,
                )
            except Exception:
                pass  # Fallback to plain text

        # 3. Parquet (Apache Parquet)
        if file_name.endswith(".parquet") or file_name.endswith(".parq"):
            size_str = format_size(total_size)
            max_str = format_size(max_bytes)

            try:
                import struct
                import pyarrow.parquet as pq

                # Если файл усечен, но передан футер (чтение по Range с конца файла)
                if truncated and footer_bytes and len(footer_bytes) >= 8 and footer_bytes.endswith(b"PAR1"):
                    try:
                        footer_len = struct.unpack("<I", footer_bytes[-8:-4])[0]
                        if len(footer_bytes) >= footer_len + 8:
                            synthetic_data = b"PAR1" + footer_bytes[-(footer_len + 8) :]
                            meta = pq.read_metadata(io.BytesIO(synthetic_data))
                            columns = list(meta.schema.names)
                            num_rows = meta.num_rows
                            num_groups = meta.num_row_groups

                            schema_lines = []
                            for i in range(len(meta.schema)):
                                col_schema = meta.schema.column(i)
                                schema_lines.append(f"  - {col_schema.name} ({col_schema.physical_type})")
                            schema_desc = "\n".join(schema_lines)

                            # Попытка извлечь строки данных из Row Group 0 через комбинированный поток
                            sample_rows = []
                            try:
                                stream = io.BufferedReader(
                                    _SeekableFooterStream(total_size, footer_bytes[-(footer_len + 8) :], header=content)
                                )
                                pf = pq.ParquetFile(stream)
                                if pf.num_row_groups > 0:
                                    rg0 = pf.read_row_group(0)
                                    pylist = rg0.slice(0, 50).to_pylist()
                                    sample_rows = [
                                        [str(r.get(c, "")) if r.get(c) is not None else "" for c in columns]
                                        for r in pylist
                                    ]
                            except Exception:
                                pass

                            return FilePreviewResponse(
                                cluster_id=cluster_id,
                                path=path,
                                file_type="parquet",
                                size=total_size,
                                truncated=True,
                                content=(
                                    f"Файл Apache Parquet (размер: {size_str}, строк: {num_rows}, групп строк: {num_groups}). "
                                    f"Размер превышает лимит полного чтения ({max_str}). "
                                    f"Схема и метаданные колонок успешно извлечены из футера файла:\n\n{schema_desc}"
                                ),
                                columns=columns,
                                rows=sample_rows,
                                row_count=len(sample_rows),
                            )
                    except Exception as fe:
                        logger.warning(f"Не удалось распарсить футер Parquet файла {path}: {fe}")

                # Если файл усечен и футер не получен
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
                        row_count=0,
                    )

                # Полное чтение файла (до max_bytes)
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
                    row_count=len(safe_rows),
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
                    row_count=0,
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
                    row_count=0,
                )

        # 4. ORC (Optimized Row Columnar)
        if file_name.endswith(".orc"):
            size_str = format_size(total_size)
            max_str = format_size(max_bytes)

            # Если файл усечен, но передан футер (чтение по Range с конца файла)
            if truncated and footer_bytes and len(footer_bytes) >= 4:
                try:
                    import pyarrow.orc as orc

                    stream = io.BufferedReader(_SeekableFooterStream(total_size, footer_bytes, header=content))
                    reader = orc.ORCFile(stream)
                    schema = reader.schema
                    columns = list(schema.names)
                    num_rows = reader.nrows
                    num_stripes = reader.nstripes

                    schema_lines = []
                    for field in schema:
                        schema_lines.append(f"  - {field.name} ({field.type})")
                    schema_desc = "\n".join(schema_lines)

                    # Попытка извлечь строки данных из Stripe 0 через комбинированный поток
                    sample_rows = []
                    try:
                        if num_stripes > 0:
                            stripe0 = reader.read_stripe(0)
                            pylist = stripe0.slice(0, 50).to_pylist()
                            sample_rows = [
                                [str(r.get(c, "")) if r.get(c) is not None else "" for c in columns] for r in pylist
                            ]
                    except Exception:
                        pass

                    return FilePreviewResponse(
                        cluster_id=cluster_id,
                        path=path,
                        file_type="orc",
                        size=total_size,
                        truncated=True,
                        content=(
                            f"Файл Apache ORC (размер: {size_str}, строк: {num_rows}, страйпов: {num_stripes}). "
                            f"Размер превышает лимит полного чтения ({max_str}). "
                            f"Схема и метаданные колонок успешно извлечены из футера файла:\n\n{schema_desc}"
                        ),
                        columns=columns,
                        rows=sample_rows,
                        row_count=len(sample_rows),
                    )
                except Exception as fe:
                    logger.warning(f"Не удалось распарсить футер ORC файла {path}: {fe}")

            # Если файл усечен и футер не получен
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
                    row_count=0,
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
                    row_count=len(safe_rows),
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
                    row_count=0,
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
                    row_count=0,
                )

        # 5. Текстовые файлы (txt, log, yaml, yml, conf, xml, properties, sql, sh, py, etc.)
        try:
            text = content.decode("utf-8")
            return FilePreviewResponse(
                cluster_id=cluster_id, path=path, file_type="text", size=total_size, truncated=truncated, content=text
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
                    content="Бинарный файл. Предпросмотр недоступен. Используйте кнопку 'Скачать'.",
                )
            return FilePreviewResponse(
                cluster_id=cluster_id,
                path=path,
                file_type="text",
                size=total_size,
                truncated=truncated,
                content=text_lossy,
            )


preview_service = PreviewService()
