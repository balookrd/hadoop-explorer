import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.common.core.metrics import (
    Counter,
    Gauge,
    Histogram,
    MetricsRegistry,
    PrometheusMetricsMiddleware,
    metrics_registry,
)


def test_counter_primitive():
    c = Counter("test_counter", "Test counter description", ["method", "status"])
    c.inc(method="GET", status="200")
    c.inc(value=2.5, method="GET", status="200")
    c.inc(method="POST", status="400")

    assert c.get(method="GET", status="200") == 3.5
    assert c.get(method="POST", status="400") == 1.0
    assert c.get(method="DELETE", status="500") == 0.0

    lines = c.collect()
    assert "# HELP test_counter Test counter description" in lines
    assert "# TYPE test_counter counter" in lines
    text = "\n".join(lines)
    assert 'test_counter{method="GET",status="200"} 3.5' in text
    assert 'test_counter{method="POST",status="400"} 1' in text

    with pytest.raises(ValueError):
        c.inc(value=-1, method="GET", status="200")


def test_gauge_primitive():
    g = Gauge("test_gauge", "Test gauge description", ["service"])
    g.set(10.0, service="hdfs")
    assert g.get(service="hdfs") == 10.0

    g.inc(2.0, service="hdfs")
    assert g.get(service="hdfs") == 12.0

    g.dec(5.0, service="hdfs")
    assert g.get(service="hdfs") == 7.0

    lines = g.collect()
    assert "# HELP test_gauge Test gauge description" in lines
    assert "# TYPE test_gauge gauge" in lines
    assert 'test_gauge{service="hdfs"} 7' in "\n".join(lines)


def test_histogram_primitive():
    h = Histogram(
        "test_latency",
        "Test latency description",
        ["endpoint"],
        buckets=(0.01, 0.05, 0.1, 1.0),
    )
    h.observe(0.005, endpoint="/api/v1/files")
    h.observe(0.03, endpoint="/api/v1/files")
    h.observe(0.08, endpoint="/api/v1/files")
    h.observe(2.0, endpoint="/api/v1/files")

    lines = h.collect()
    text = "\n".join(lines)
    assert "# HELP test_latency Test latency description" in lines
    assert "# TYPE test_latency histogram" in lines
    assert 'test_latency_bucket{endpoint="/api/v1/files",le="0.01"} 1' in text
    assert 'test_latency_bucket{endpoint="/api/v1/files",le="0.05"} 2' in text
    assert 'test_latency_bucket{endpoint="/api/v1/files",le="0.1"} 3' in text
    assert 'test_latency_bucket{endpoint="/api/v1/files",le="1"} 3' in text
    assert 'test_latency_bucket{endpoint="/api/v1/files",le="+Inf"} 4' in text
    assert 'test_latency_count{endpoint="/api/v1/files"} 4' in text


def test_metrics_registry_and_formatting():
    reg = MetricsRegistry()
    c = reg.counter("reg_counter", "Desc", ["app"])
    c.inc(app="test-app")

    g = reg.gauge("reg_gauge", "Desc", ["app"])
    g.set(42.0, app="test-app")

    prom_text = reg.format_prometheus_metrics()
    assert "reg_counter" in prom_text
    assert "reg_gauge" in prom_text
    assert 'reg_counter{app="test-app"} 1' in prom_text
    assert 'reg_gauge{app="test-app"} 42' in prom_text


def test_prometheus_middleware_integration():
    metrics_registry.reset()

    app = FastAPI(title="test-app")
    app.add_middleware(PrometheusMetricsMiddleware, app_name="test-app")

    @app.get("/api/v1/items/{item_id}")
    async def get_item(item_id: str):
        return {"item": item_id}

    @app.post("/api/v1/action")
    async def do_action():
        return {"status": "done"}

    @app.get("/metrics")
    async def get_metrics():
        from fastapi.responses import Response

        return Response(
            content=metrics_registry.format_prometheus_metrics(),
            media_type="text/plain",
        )

    client = TestClient(app)

    # 1. Вызов GET эндпоинта
    r1 = client.get("/api/v1/items/12345")
    assert r1.status_code == 200

    # 2. Вызов POST эндпоинта
    r2 = client.post("/api/v1/action")
    assert r2.status_code == 200

    # 3. Вызов 404 эндпоинта
    r3 = client.get("/api/v1/nonexistent/999")
    assert r3.status_code == 404

    # 4. Проверяем /metrics
    rm = client.get("/metrics")
    assert rm.status_code == 200
    metrics_content = rm.text

    # Проверяем нормализацию путей
    assert 'http_requests_total{app="test-app",method="GET",path="/api/v1/items/{item_id}",status="200"} 1' in metrics_content
    assert 'http_requests_total{app="test-app",method="POST",path="/api/v1/action",status="200"} 1' in metrics_content
    assert "http_request_duration_seconds_bucket" in metrics_content
    assert "http_requests_in_progress" in metrics_content
