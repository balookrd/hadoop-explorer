import time
import pytest
import anyio
from unittest.mock import MagicMock, patch

from app.core.config import ClusterConfig, ImpersonationConfig
from app.services.trino_engine import (
    MetadataTTLCache,
    safe_ident,
    TrinoExecutionEngine,
)
from app.services.hive_engine import (
    HiveMetadataTTLCache,
    safe_hive_ident,
    HiveExecutionEngine,
)


@pytest.fixture
def mock_trino_cluster():
    return ClusterConfig(
        id="trino-test",
        name="Trino Test",
        type="trino",
        host="trino.local",
        port=8080,
        auth={"type": "none"},
        impersonation=ImpersonationConfig(enabled=True),
        use_ssl=False,
    )


@pytest.fixture
def mock_hive_cluster():
    return ClusterConfig(
        id="hive-test",
        name="Hive Test",
        type="hive",
        host="hive.local",
        port=10000,
        auth={"type": "none"},
        impersonation=ImpersonationConfig(enabled=True),
        use_ssl=False,
    )


def test_metadata_ttl_cache_lifecycle():
    cache = MetadataTTLCache(default_ttl=10.0)

    # 1. Set & Get
    cache.set("k1", "val1")
    assert cache.get("k1") == "val1"
    assert cache.get("k_unknown") is None

    # 2. TTL expiration
    cache.set("k_exp", "val_exp", ttl=-1.0)
    assert cache.get("k_exp") is None

    # 3. Invalidate prefix
    cache.set("prefix:1", "a")
    cache.set("prefix:2", "b")
    cache.set("other:1", "c")
    cache.invalidate("prefix:")
    assert cache.get("prefix:1") is None
    assert cache.get("other:1") == "c"

    # 4. Clear all
    cache.clear()
    assert cache.get("other:1") is None


def test_safe_identifiers():
    # Trino
    assert safe_ident("users") == '"users"'
    assert safe_ident("order_items_2026") == '"order_items_2026"'
    with pytest.raises(ValueError):
        safe_ident("table; DROP")

    # Hive
    assert safe_hive_ident("users") == "`users`"
    assert safe_hive_ident("events_raw") == "`events_raw`"
    with pytest.raises(ValueError):
        safe_hive_ident("db.table--malicious")


@pytest.mark.asyncio
async def test_trino_execute_query_stream(mock_trino_cluster):
    engine = TrinoExecutionEngine(mock_trino_cluster)

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id", "integer"), ("name", "varchar")]
    mock_cursor.fetchmany.side_effect = [
        [(1, "Alice"), (2, "Bob")],
        [],  # EOF
    ]
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(engine, "_get_connection", return_value=mock_conn):
        events = []
        async for event in engine.execute_query("SELECT id, name FROM users", user_login="analyst"):
            events.append(event)

        event_types = [e["type"] for e in events]
        assert "status" in event_types
        assert "columns" in event_types
        assert "rows" in event_types
        assert "finished" in event_types

        # Проверяем колонки и строки
        cols_event = next(e for e in events if e["type"] == "columns")
        assert len(cols_event["columns"]) == 2
        assert cols_event["columns"][0]["name"] == "id"

        rows_event = next(e for e in events if e["type"] == "rows")
        assert len(rows_event["rows"]) == 2
        assert rows_event["rows"][0] == [1, "Alice"]


@pytest.mark.asyncio
async def test_trino_execute_query_cancellation(mock_trino_cluster):
    engine = TrinoExecutionEngine(mock_trino_cluster)

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id", "integer")]
    mock_conn.cursor.return_value = mock_cursor

    cancel_event = anyio.Event()
    cancel_event.set()  # Сразу отменен

    with patch.object(engine, "_get_connection", return_value=mock_conn):
        events = []
        async for event in engine.execute_query(
            "SELECT * FROM big_data", user_login="analyst", cancel_event=cancel_event
        ):
            events.append(event)

        cancel_events = [e for e in events if e.get("status") == "CANCELLED"]
        assert len(cancel_events) > 0
        assert "отменен" in cancel_events[0]["message"]


@pytest.mark.asyncio
async def test_hive_execute_query_stream(mock_hive_cluster):
    engine = HiveExecutionEngine(mock_hive_cluster)

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("event_id", "string")]
    mock_cursor.fetchmany.side_effect = [
        [("evt-001",)],
        [],
    ]
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(engine, "_get_connection", return_value=mock_conn):
        events = []
        async for event in engine.execute_query("SELECT event_id FROM logs", user_login="engineer"):
            events.append(event)

        event_types = [e["type"] for e in events]
        assert "status" in event_types
        assert "columns" in event_types
        assert "rows" in event_types
        assert "finished" in event_types
