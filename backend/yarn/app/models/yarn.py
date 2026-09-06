from enum import Enum
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field, field_validator


class QueueType(str, Enum):
    FIXED = "fixed"
    ELASTIC = "elastic"


class QueueState(str, Enum):
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    DRAINING = "DRAINING"


class ResourceAllocation(BaseModel):
    memory_mb: int = 0
    vcores: int = 0


class PartitionResourceConfig(BaseModel):
    partition_name: str = "DEFAULT"
    capacity: float = Field(..., description="Гарантированная емкость (%)")
    max_capacity: float = Field(..., description="Максимальная емкость (%)")
    is_elastic: bool = Field(False, description="Эластичная ли очередь (max_capacity > capacity)")
    elasticity_ratio: float = Field(1.0, description="Коэффициент эластичности (max_capacity / capacity)")
    
    # Раздельные ресурсы RAM и vCPU
    memory_mb: Optional[int] = Field(None, description="Гарантированная память (RAM) в MB")
    vcores: Optional[int] = Field(None, description="Гарантированные ядра (vCPU)")
    max_memory_mb: Optional[int] = Field(None, description="Максимальная память (RAM) в MB")
    max_vcores: Optional[int] = Field(None, description="Максимальные ядра (vCPU)")
    
    memory_percent: Optional[float] = Field(None, description="Процент памяти (RAM) от родителя")
    vcore_percent: Optional[float] = Field(None, description="Процент ядер (vCPU) от родителя")
    max_memory_percent: Optional[float] = Field(None, description="Макс. процент памяти (RAM) от родителя")
    max_vcore_percent: Optional[float] = Field(None, description="Макс. процент ядер (vCPU) от родителя")

    absolute_resources: Optional[ResourceAllocation] = None
    absolute_max_resources: Optional[ResourceAllocation] = None


class QueueNode(BaseModel):
    name: str
    path: str
    parent_path: Optional[str] = None
    is_leaf: bool = True
    state: QueueState = QueueState.RUNNING
    resource_mode: str = "percentage"
    user_limit_factor: Optional[float] = 1.0
    ordering_policy: Optional[str] = "fifo"
    max_applications: Optional[int] = None
    max_am_resource_percent: Optional[float] = None
    max_parallel_apps: Optional[int] = None
    max_application_lifetime: Optional[int] = None
    accessible_node_labels: Optional[List[str]] = None
    default_node_label_expression: Optional[str] = None
    partitions: Dict[str, PartitionResourceConfig] = Field(default_factory=dict)
    
    # Текущие метрики использования из YARN RM
    current_used_resources: ResourceAllocation = Field(default_factory=ResourceAllocation)
    allocated_resources: ResourceAllocation = Field(default_factory=ResourceAllocation)
    current_used_percent: float = 0.0
    num_applications: int = 0
    num_active_applications: int = 0
    num_pending_applications: int = 0
    
    children: List["QueueNode"] = Field(default_factory=list)


class ClusterMetrics(BaseModel):
    total_memory_mb: int
    total_vcores: int
    allocated_memory_mb: int
    allocated_vcores: int
    available_memory_mb: int
    available_vcores: int
    active_nodes: int
    unhealthy_nodes: int
    total_containers: int
    running_apps: int
    partitions: List[str]


class BranchBalance(BaseModel):
    parent_path: str
    partition: str
    total_children_capacity: float
    unallocated_capacity: float
    is_balanced: bool
    status: str  # "ok", "underallocated", "overallocated"
    message: str
    
    # Раздельный баланс по RAM и vCPU
    total_children_memory_mb: Optional[int] = None
    unallocated_memory_mb: Optional[int] = None
    total_children_vcores: Optional[int] = None
    unallocated_vcores: Optional[int] = None
    ram_is_balanced: Optional[bool] = None
    vcpu_is_balanced: Optional[bool] = None


class QueueTreeResponse(BaseModel):
    cluster_id: str
    cluster_name: str
    resource_mode: str
    default_partition: str
    partitions: List[str]
    root_queue: QueueNode
    cluster_metrics: ClusterMetrics
    balances: List[BranchBalance]
    queue_mappings: Optional[str] = None
    queue_mappings_override: bool = False


# Модели для черновиков (Draft), Diff и генерации XML
class QueueDraftItem(BaseModel):
    path: str = Field(..., pattern=r"^root(\.[a-zA-Z0-9_\-]+)*$", description="Полный путь очереди, начиная с root")
    name: str = Field(..., pattern=r"^[a-zA-Z0-9_\-]+$", description="Имя очереди (буквы, цифры, дефис, подчеркивание)")
    parent_path: Optional[str] = Field(None, pattern=r"^root(\.[a-zA-Z0-9_\-]+)*$", description="Путь родительской очереди")
    action: str = "modify"  # modify | create | delete
    is_leaf: bool = True
    state: QueueState = QueueState.RUNNING
    resource_mode: Optional[str] = None
    user_limit_factor: Optional[float] = None
    ordering_policy: Optional[str] = None
    max_applications: Optional[int] = None
    max_am_resource_percent: Optional[float] = None
    max_parallel_apps: Optional[int] = None
    max_application_lifetime: Optional[int] = None
    accessible_node_labels: Optional[List[str]] = None
    default_node_label_expression: Optional[str] = None
    partitions: Dict[str, PartitionResourceConfig]


DraftQueueItem = QueueDraftItem


def validate_yarn_queue_mappings(v: Optional[str]) -> Optional[str]:
    if v is None or not v.strip():
        return v
    import re
    entries = [e.strip() for e in v.split(",") if e.strip()]
    mapping_regex = re.compile(r"^(u|g):[%a-zA-Z0-9_\-\.\*]+:[a-zA-Z0-9_\-\.\%]+$")
    for entry in entries:
        if not mapping_regex.match(entry):
            raise ValueError(
                f"Некорректный формат маппинга очередей: '{entry}'. "
                "Ожидается формат 'u:user:queue' или 'g:group:queue' (например, 'u:%user:%user,u:analyst:root.analytics')."
            )
    return v


class DraftValidateRequest(BaseModel):
    cluster_id: str
    selected_partition: str = "DEFAULT"
    queues: List[QueueDraftItem]
    queue_mappings: Optional[str] = None
    queue_mappings_override: Optional[bool] = None

    @field_validator("queue_mappings")
    @classmethod
    def check_queue_mappings(cls, v: Optional[str]) -> Optional[str]:
        return validate_yarn_queue_mappings(v)


class DraftValidateResponse(BaseModel):
    is_valid: bool
    balances: List[BranchBalance]
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class DiffItem(BaseModel):
    path: str
    name: str
    parent_path: Optional[str] = None
    partition: str
    action: str  # created, modified, deleted, unchanged
    
    live_capacity: Optional[float] = None
    draft_capacity: Optional[float] = None
    delta_capacity: Optional[float] = None
    
    live_max_capacity: Optional[float] = None
    draft_max_capacity: Optional[float] = None
    delta_max_capacity: Optional[float] = None
    
    # RAM diff
    live_memory_mb: Optional[int] = None
    draft_memory_mb: Optional[int] = None
    delta_memory_mb: Optional[int] = None

    # vCPU diff
    live_vcores: Optional[int] = None
    draft_vcores: Optional[int] = None
    delta_vcores: Optional[int] = None

    live_type: Optional[QueueType] = None
    draft_type: Optional[QueueType] = None
    
    live_state: Optional[QueueState] = None
    draft_state: Optional[QueueState] = None

    live_resource_mode: Optional[str] = None
    draft_resource_mode: Optional[str] = None

    live_user_limit_factor: Optional[float] = None
    draft_user_limit_factor: Optional[float] = None
    live_ordering_policy: Optional[str] = None
    draft_ordering_policy: Optional[str] = None

    live_max_applications: Optional[int] = None
    draft_max_applications: Optional[int] = None
    live_max_am_resource_percent: Optional[float] = None
    draft_max_am_resource_percent: Optional[float] = None
    live_max_parallel_apps: Optional[int] = None
    draft_max_parallel_apps: Optional[int] = None
    live_max_application_lifetime: Optional[int] = None
    draft_max_application_lifetime: Optional[int] = None


class DraftDiffResponse(BaseModel):
    cluster_id: str
    has_changes: bool
    diffs: List[DiffItem]
    queue_mappings_diff: Optional[Dict[str, Any]] = None


class GenerateXmlRequest(BaseModel):
    cluster_id: str
    queues: List[QueueDraftItem]
    proposal_comment: Optional[str] = None
    resource_mode_override: Optional[str] = None  # percentage | absolute
    queue_mappings: Optional[str] = None
    queue_mappings_override: Optional[bool] = None

    @field_validator("queue_mappings")
    @classmethod
    def check_queue_mappings(cls, v: Optional[str]) -> Optional[str]:
        return validate_yarn_queue_mappings(v)


class GenerateXmlResponse(BaseModel):
    cluster_id: str
    filename: str
    xml_content: str
    applied_by: str
    generated_at: str
    instructions: str
