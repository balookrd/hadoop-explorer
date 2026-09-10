"""
Модели данных для визуального конструктора и планировщика DAG-пайплайнов Spark.
Включает топологическую валидацию графа на отсутствие циклов.
"""

from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
import time
import uuid


class PipelineNodeType(str, Enum):
    PYSPARK = "pyspark"
    SPARK_SQL = "spark_sql"
    SPARK_SUBMIT = "spark_submit"
    WAIT = "wait"


class NodePosition(BaseModel):
    x: float = 0.0
    y: float = 0.0


class PipelineNode(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str
    type: PipelineNodeType = PipelineNodeType.PYSPARK
    code: str = ""
    timeout_seconds: int = 300
    config: Dict[str, Any] = Field(default_factory=dict)
    position: NodePosition = Field(default_factory=NodePosition)


class PipelineEdge(BaseModel):
    from_node_id: str
    to_node_id: str


class PipelineDefinition(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str
    description: Optional[str] = ""
    cluster_id: str
    nodes: List[PipelineNode] = Field(default_factory=list)
    edges: List[PipelineEdge] = Field(default_factory=list)
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)

    def validate_dag(self) -> None:
        """
        Проверяет, что граф является направленным ациклическим (DAG).
        Возбуждает ValueError при обнаружении цикла или невалидных ребер.
        """
        node_ids = {n.id for n in self.nodes}
        adj: Dict[str, List[str]] = {n.id: [] for n in self.nodes}
        in_degree: Dict[str, int] = {n.id: 0 for n in self.nodes}

        for edge in self.edges:
            if edge.from_node_id not in node_ids:
                raise ValueError(f"Edge references non-existent source node: {edge.from_node_id}")
            if edge.to_node_id not in node_ids:
                raise ValueError(f"Edge references non-existent target node: {edge.to_node_id}")
            adj[edge.from_node_id].append(edge.to_node_id)
            in_degree[edge.to_node_id] += 1

        # Алгоритм Кана (Kahn's Algorithm) для проверки циклов
        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        visited_count = 0

        while queue:
            curr = queue.pop(0)
            visited_count += 1
            for neighbor in adj[curr]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if visited_count != len(self.nodes):
            raise ValueError("Обнаружен цикл в графе пайплайна! Граф должен быть строго ациклическим (DAG).")


class NodeExecutionState(BaseModel):
    node_id: str
    status: str = "PENDING"  # PENDING, RUNNING, SUCCESS, FAILED, SKIPPED
    output: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None


class PipelineRun(BaseModel):
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    pipeline_id: str
    status: str = "RUNNING"  # RUNNING, SUCCESS, FAILED
    node_states: Dict[str, NodeExecutionState] = Field(default_factory=dict)
    started_at: float = Field(default_factory=time.time)
    finished_at: Optional[float] = None
