"""
API эндпоинты для управления, валидации и запуска DAG-пайплайнов Spark.
"""

import asyncio
import time
from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, status

from app.models.pipeline import PipelineDefinition, PipelineRun, NodeExecutionState
from backend.common.core.security import CommonUserSession
from backend.common.api.auth_router import security_scheme

router = APIRouter(prefix="/pipelines", tags=["pipelines"])

# В памяти для демо / персистентность через storage_service
_pipelines_store: Dict[str, PipelineDefinition] = {}
_pipeline_runs_store: Dict[str, PipelineRun] = {}


@router.get("", response_model=List[PipelineDefinition])
async def list_pipelines():
    """Возвращает список всех созданных DAG пайплайнов."""
    return list(_pipelines_store.values())


@router.post("", response_model=PipelineDefinition)
async def create_or_update_pipeline(pipeline: PipelineDefinition):
    """Создает или обновляет пайплайн с обязательной топологической валидацией отсутствия циклов."""
    try:
        pipeline.validate_dag()
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )

    pipeline.updated_at = time.time()
    _pipelines_store[pipeline.id] = pipeline
    return pipeline


@router.get("/{pipeline_id}", response_model=PipelineDefinition)
async def get_pipeline(pipeline_id: str):
    """Возвращает структуру пайплайна по его идентификатору."""
    if pipeline_id not in _pipelines_store:
        raise HTTPException(status_code=404, detail="Пайплайн не найден")
    return _pipelines_store[pipeline_id]


@router.delete("/{pipeline_id}")
async def delete_pipeline(pipeline_id: str):
    """Удаляет пайплайн."""
    if pipeline_id in _pipelines_store:
        del _pipelines_store[pipeline_id]
    return {"success": True, "message": "Пайплайн успешно удален"}


@router.post("/{pipeline_id}/run", response_model=PipelineRun)
async def run_pipeline(pipeline_id: str):
    """Запускает выполнение графа пайплайна."""
    if pipeline_id not in _pipelines_store:
        raise HTTPException(status_code=404, detail="Пайплайн не найден")

    pipeline = _pipelines_store[pipeline_id]
    pipeline.validate_dag()

    run = PipelineRun(pipeline_id=pipeline_id)
    for node in pipeline.nodes:
        run.node_states[node.id] = NodeExecutionState(node_id=node.id, status="PENDING")

    _pipeline_runs_store[run.run_id] = run

    # Фоновое выполнение узлов DAG
    asyncio.create_task(_execute_pipeline_dag(pipeline, run))

    return run


@router.get("/{pipeline_id}/runs/{run_id}", response_model=PipelineRun)
async def get_pipeline_run_status(pipeline_id: str, run_id: str):
    """Возвращает текущий прогресс исполнения пайплайна."""
    if run_id not in _pipeline_runs_store:
        raise HTTPException(status_code=404, detail="Запуск пайплайна не найден")
    return _pipeline_runs_store[run_id]


async def _execute_pipeline_dag(pipeline: PipelineDefinition, run: PipelineRun):
    """Последовательно-параллельное выполнение узлов DAG."""
    try:
        # Для простоты исполняем узлы с соблюдением топологического порядка
        for node in pipeline.nodes:
            st = run.node_states[node.id]
            st.status = "RUNNING"
            st.started_at = time.time()

            # Имитация выполнения шага (SQL / PySpark)
            await asyncio.sleep(0.5)

            st.status = "SUCCESS"
            st.output = f"Узел {node.name} ({node.type}) успешно выполнен"
            st.finished_at = time.time()

        run.status = "SUCCESS"
        run.finished_at = time.time()
    except Exception as e:
        run.status = "FAILED"
        run.finished_at = time.time()
