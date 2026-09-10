import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.models.pipeline import PipelineDefinition, PipelineNode, PipelineEdge, PipelineNodeType


@pytest.mark.asyncio
async def test_dag_cycle_detection():
    # Валидный линейный DAG (1 -> 2 -> 3)
    p_valid = PipelineDefinition(
        name="Valid Pipeline",
        cluster_id="dev-hadoop",
        nodes=[
            PipelineNode(id="n1", name="Step 1", type=PipelineNodeType.SPARK_SQL),
            PipelineNode(id="n2", name="Step 2", type=PipelineNodeType.PYSPARK),
            PipelineNode(id="n3", name="Step 3", type=PipelineNodeType.PYSPARK),
        ],
        edges=[
            PipelineEdge(from_node_id="n1", to_node_id="n2"),
            PipelineEdge(from_node_id="n2", to_node_id="n3"),
        ],
    )
    # Не должно вызывать исключений
    p_valid.validate_dag()

    # Невалидный циклический DAG (1 -> 2 -> 3 -> 1)
    p_cyclic = PipelineDefinition(
        name="Cyclic Pipeline",
        cluster_id="dev-hadoop",
        nodes=[
            PipelineNode(id="n1", name="Step 1", type=PipelineNodeType.SPARK_SQL),
            PipelineNode(id="n2", name="Step 2", type=PipelineNodeType.PYSPARK),
            PipelineNode(id="n3", name="Step 3", type=PipelineNodeType.PYSPARK),
        ],
        edges=[
            PipelineEdge(from_node_id="n1", to_node_id="n2"),
            PipelineEdge(from_node_id="n2", to_node_id="n3"),
            PipelineEdge(from_node_id="n3", to_node_id="n1"),
        ],
    )
    with pytest.raises(ValueError, match="Обнаружен цикл в графе пайплайна"):
        p_cyclic.validate_dag()


@pytest.mark.asyncio
async def test_pipeline_api_lifecycle():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Создание валидного пайплайна
        payload = {
            "name": "Daily Aggregation Pipeline",
            "cluster_id": "dev-hadoop",
            "nodes": [
                {"id": "step1", "name": "Extract", "type": "spark_sql", "code": "SELECT 1"},
                {"id": "step2", "name": "Transform", "type": "pyspark", "code": "df = spark.createDataFrame([])"},
            ],
            "edges": [{"from_node_id": "step1", "to_node_id": "step2"}],
        }
        create_resp = await client.post("/api/pipelines", json=payload)
        assert create_resp.status_code == 200
        data = create_resp.json()
        pipeline_id = data["id"]
        assert len(data["nodes"]) == 2

        # 2. Попытка создать циклический пайплайн через API (должен вернуть 422)
        cyclic_payload = {
            "name": "Broken Cycle",
            "cluster_id": "dev-hadoop",
            "nodes": [
                {"id": "a", "name": "A", "type": "pyspark"},
                {"id": "b", "name": "B", "type": "pyspark"},
            ],
            "edges": [
                {"from_node_id": "a", "to_node_id": "b"},
                {"from_node_id": "b", "to_node_id": "a"},
            ],
        }
        err_resp = await client.post("/api/pipelines", json=cyclic_payload)
        assert err_resp.status_code == 422

        # 3. Запуск пайплайна
        run_resp = await client.post(f"/api/pipelines/{pipeline_id}/run")
        assert run_resp.status_code == 200
        run_data = run_resp.json()
        assert run_data["pipeline_id"] == pipeline_id
        assert run_data["status"] in ("RUNNING", "SUCCESS")
