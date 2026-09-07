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
