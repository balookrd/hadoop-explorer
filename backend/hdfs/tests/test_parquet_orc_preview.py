import pytest
import io
from app.services.preview import preview_service, format_size
from app.models.hdfs import FilePreviewResponse


def test_format_size():
    assert format_size(500) == "500 B"
    assert format_size(2048) == "2.0 KB"
    assert format_size(5 * 1024 * 1024) == "5.0 MB"
    assert format_size(2 * 1024 * 1024 * 1024) == "2.00 GB"


def test_parquet_truncated_preview():
    """
    Проверяет, что усеченный Parquet файл (размер > max_bytes)
    возвращает информативный структурированный ответ без падения.
    """
    total_size = 100 * 1024 * 1024  # 100 МБ
    max_bytes = 1024 * 1024  # 1 МБ
    content = b"PAR1" + b"\x00" * 1000

    resp = preview_service.generate_preview(
        path="/data/large_dataset.parquet",
        cluster_id="prod-cluster",
        content=content,
        total_size=total_size,
        max_bytes=max_bytes,
    )

    assert isinstance(resp, FilePreviewResponse)
    assert resp.file_type == "parquet"
    assert resp.truncated is True
    assert resp.size == total_size
    assert "превышает лимит предварительного просмотра" in resp.content
    assert "Скачайте файл" in resp.content
    assert resp.row_count == 0


def test_orc_truncated_preview():
    """
    Проверяет, что усеченный ORC файл возвращает структурированный ответ.
    """
    total_size = 50 * 1024 * 1024
    max_bytes = 1024 * 1024
    content = b"ORC" + b"\x00" * 500

    resp = preview_service.generate_preview(
        path="/data/large_table.orc",
        cluster_id="prod-cluster",
        content=content,
        total_size=total_size,
        max_bytes=max_bytes,
    )

    assert isinstance(resp, FilePreviewResponse)
    assert resp.file_type == "orc"
    assert resp.truncated is True
    assert resp.size == total_size
    assert "превышает лимит предварительного просмотра" in resp.content
    assert "Скачайте файл" in resp.content


def test_corrupted_parquet_preview():
    """
    Проверяет обработку поврежденного Parquet файла (невалидный footer).
    """
    total_size = 500
    max_bytes = 10000
    corrupted_content = b"INVALID_PARQUET_MAGIC_BYTES_DATA"

    resp = preview_service.generate_preview(
        path="/data/corrupted.parquet",
        cluster_id="prod-cluster",
        content=corrupted_content,
        total_size=total_size,
        max_bytes=max_bytes,
    )

    assert isinstance(resp, FilePreviewResponse)
    assert resp.file_type == "parquet"
    assert resp.row_count == 0
    assert "Не удалось прочитать структуру Parquet" in resp.content or "pyarrow" in resp.content


def test_parquet_footer_schema_preview():
    """
    Проверяет извлечение схемы и метаданных колонок из футера для большого Parquet-файла.
    """
    import struct
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Создаем тестовый Parquet датасет
    table = pa.Table.from_arrays(
        [
            pa.array([101, 102, 103]),
            pa.array(["sales", "marketing", "engineering"]),
            pa.array([99.5, 120.0, 150.75]),
        ],
        names=["department_id", "department_name", "budget_k"],
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    full_parquet = buf.getvalue()

    total_size = len(full_parquet) + 100 * 1024 * 1024  # Эмулируем файл 100 МБ
    max_bytes = 100  # Эмулируем маленький лимит preview_max_bytes

    # Извлекаем футер
    footer_len = struct.unpack("<I", full_parquet[-8:-4])[0]
    footer_bytes = full_parquet[-(footer_len + 8) :]

    resp = preview_service.generate_preview(
        path="/datalake/analytics/departments.parquet",
        cluster_id="prod-cluster",
        content=full_parquet[:max_bytes],
        total_size=total_size,
        max_bytes=max_bytes,
        footer_bytes=footer_bytes,
    )

    assert isinstance(resp, FilePreviewResponse)
    assert resp.file_type == "parquet"
    assert resp.truncated is True
    assert resp.columns == ["department_id", "department_name", "budget_k"]
    assert "Схема и метаданные колонок успешно извлечены из футера" in resp.content
    assert "department_id" in resp.content
    assert "department_name" in resp.content
    assert "budget_k" in resp.content


def test_orc_footer_schema_preview():
    """
    Проверяет извлечение схемы и метаданных колонок из футера для большого ORC-файла.
    """
    import pyarrow as pa
    import pyarrow.orc as orc

    table = pa.Table.from_arrays(
        [
            pa.array([201, 202, 203]),
            pa.array(["user_a", "user_b", "user_c"]),
            pa.array([12.5, 45.0, 78.9]),
        ],
        names=["user_id", "user_name", "activity_score"],
    )
    buf = io.BytesIO()
    orc.write_table(table, buf)
    full_orc = buf.getvalue()

    total_size = len(full_orc) + 50 * 1024 * 1024  # Эмулируем файл 50 МБ
    max_bytes = 100  # Эмулируем маленький лимит preview_max_bytes

    # Извлекаем последние байты (футер)
    footer_bytes = full_orc[-256:]

    resp = preview_service.generate_preview(
        path="/datalake/analytics/users.orc",
        cluster_id="prod-cluster",
        content=full_orc[:max_bytes],
        total_size=total_size,
        max_bytes=max_bytes,
        footer_bytes=footer_bytes,
    )

    assert isinstance(resp, FilePreviewResponse)
    assert resp.file_type == "orc"
    assert resp.truncated is True
    assert resp.columns == ["user_id", "user_name", "activity_score"]
    assert "Схема и метаданные колонок успешно извлечены из футера" in resp.content
    assert "user_id" in resp.content
    assert "user_name" in resp.content
    assert "activity_score" in resp.content
