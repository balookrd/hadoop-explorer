import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, status

from app.core.config import settings
from app.core.security import get_current_user
from app.core.acl import check_cluster_permission, resolve_cluster_role
from app.models.auth import UserSession, Role
from app.models.cluster import ClusterConfig
from app.models.yarn import (
    QueueTreeResponse, DraftValidateRequest, DraftValidateResponse,
    GenerateXmlRequest, GenerateXmlResponse, DraftDiffResponse, DiffItem,
)
from app.services.mock_yarn import get_mock_queue_tree, get_mock_cluster_metrics
from app.services.capacity_scheduler import validate_queue_balance, compute_balances_from_tree
from app.services.xml_generator import generate_capacity_scheduler_xml

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/clusters", tags=["queues"])


def _find_cluster(cluster_id: str) -> ClusterConfig:
    for c in settings.clusters:
        if c.id == cluster_id:
            return c
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Кластер '{cluster_id}' не найден",
    )


@router.get("/{cluster_id}/queues", response_model=QueueTreeResponse)
async def get_queue_tree(cluster_id: str, user: UserSession = Depends(get_current_user)):
    """Получает дерево очередей и метрики кластера. Доступно: reader, writer, admin."""
    cluster = _find_cluster(cluster_id)
    check_cluster_permission(user, cluster, Role.READER)

    # Для mock/dev режима используем mock данные
    if settings.auth.mode == "mock":
        root_queue = get_mock_queue_tree(cluster)
        metrics = get_mock_cluster_metrics(cluster)
    else:
        from app.services.yarn_client import YarnClient
        client = YarnClient(cluster)
        root_queue, metrics = await client.get_queue_tree(user.username)

    # Вычисляем балансы
    balances = compute_balances_from_tree(root_queue, cluster.default_partition)

    queue_mappings = getattr(cluster, "queue_mappings", None)
    queue_mappings_override = getattr(cluster, "queue_mappings_override", False)

    return QueueTreeResponse(
        cluster_id=cluster.id,
        cluster_name=cluster.name,
        resource_mode=cluster.resource_mode,
        default_partition=cluster.default_partition,
        partitions=cluster.partitions,
        root_queue=root_queue,
        cluster_metrics=metrics,
        balances=balances,
        queue_mappings=queue_mappings,
        queue_mappings_override=queue_mappings_override,
    )


@router.post("/{cluster_id}/validate", response_model=DraftValidateResponse)
async def validate_draft(
    cluster_id: str,
    body: DraftValidateRequest,
    user: UserSession = Depends(get_current_user),
):
    """Валидирует черновик изменений. Доступно: writer, admin."""
    cluster = _find_cluster(cluster_id)
    check_cluster_permission(user, cluster, Role.WRITER)

    balances = validate_queue_balance(
        queues=body.queues,
        resource_mode=cluster.resource_mode,
        partition=body.selected_partition,
    )

    errors = [b.message for b in balances if not b.is_balanced]
    warnings = []

    return DraftValidateResponse(
        is_valid=len(errors) == 0,
        balances=balances,
        errors=errors,
        warnings=warnings,
    )


@router.post("/{cluster_id}/diff", response_model=DraftDiffResponse)
async def get_diff(
    cluster_id: str,
    body: DraftValidateRequest,
    user: UserSession = Depends(get_current_user),
):
    """Вычисляет diff между live и draft. Доступно: writer, admin."""
    cluster = _find_cluster(cluster_id)
    check_cluster_permission(user, cluster, Role.WRITER)

    # Загружаем текущее состояние
    if settings.auth.mode == "mock":
        live_root = get_mock_queue_tree(cluster)
    else:
        from app.services.yarn_client import YarnClient
        client = YarnClient(cluster)
        live_root, _ = await client.get_queue_tree(user.username)

    # Индексируем live очереди
    live_map = {}

    def index_live(node):
        live_map[node.path] = node
        for child in node.children:
            index_live(child)

    index_live(live_root)

    # Строим diff
    diffs = []
    draft_paths = set()

    for draft_q in body.queues:
        draft_paths.add(draft_q.path)
        live_q = live_map.get(draft_q.path)
        partition = body.selected_partition

        draft_part = draft_q.partitions.get(partition)
        live_part = live_q.partitions.get(partition) if live_q else None

        live_mode = getattr(live_q, "resource_mode", None) if live_q else None
        draft_mode = draft_q.resource_mode or live_mode or cluster.resource_mode

        live_ulf = getattr(live_q, "user_limit_factor", None) if live_q else None
        draft_ulf = draft_q.user_limit_factor
        live_ordering = getattr(live_q, "ordering_policy", None) if live_q else None
        draft_ordering = draft_q.ordering_policy

        live_max_apps = getattr(live_q, "max_applications", None) if live_q else None
        draft_max_apps = draft_q.max_applications

        live_max_am = getattr(live_q, "max_am_resource_percent", None) if live_q else None
        draft_max_am = draft_q.max_am_resource_percent

        live_max_parallel = getattr(live_q, "max_parallel_apps", None) if live_q else None
        draft_max_parallel = draft_q.max_parallel_apps

        live_lifetime = getattr(live_q, "max_application_lifetime", None) if live_q else None
        draft_lifetime = draft_q.max_application_lifetime

        if draft_q.action == "create":
            action = "created"
        elif draft_q.action == "delete":
            action = "deleted"
        elif live_q:
            # Проверяем есть ли изменения
            has_changes = False
            if draft_part and live_part:
                if (abs(draft_part.capacity - live_part.capacity) > 0.01 or
                    abs(draft_part.max_capacity - live_part.max_capacity) > 0.01):
                    has_changes = True
            if live_q.state != draft_q.state:
                has_changes = True
            if draft_q.resource_mode and live_mode and draft_q.resource_mode != live_mode:
                has_changes = True
            if draft_ulf is not None and live_ulf is not None and abs(draft_ulf - live_ulf) > 0.001:
                has_changes = True
            if draft_ordering and live_ordering and draft_ordering.lower() != live_ordering.lower():
                has_changes = True
            if draft_max_apps is not None and draft_max_apps != live_max_apps:
                has_changes = True
            if draft_max_am is not None and (live_max_am is None or abs(draft_max_am - live_max_am) > 0.001):
                has_changes = True
            if draft_max_parallel is not None and draft_max_parallel != live_max_parallel:
                has_changes = True
            if draft_lifetime is not None and draft_lifetime != live_lifetime:
                has_changes = True
            action = "modified" if has_changes else "unchanged"
        else:
            action = "created"

        diffs.append(DiffItem(
            path=draft_q.path,
            name=draft_q.name,
            parent_path=draft_q.parent_path,
            partition=partition,
            action=action,
            live_capacity=live_part.capacity if live_part else None,
            draft_capacity=draft_part.capacity if draft_part else None,
            delta_capacity=(
                round(draft_part.capacity - live_part.capacity, 2)
                if draft_part and live_part else None
            ),
            live_max_capacity=live_part.max_capacity if live_part else None,
            draft_max_capacity=draft_part.max_capacity if draft_part else None,
            delta_max_capacity=(
                round(draft_part.max_capacity - live_part.max_capacity, 2)
                if draft_part and live_part else None
            ),
            live_memory_mb=live_part.memory_mb if live_part else None,
            draft_memory_mb=draft_part.memory_mb if draft_part else None,
            delta_memory_mb=(
                draft_part.memory_mb - live_part.memory_mb
                if draft_part and live_part and draft_part.memory_mb is not None and live_part.memory_mb is not None
                else None
            ),
            live_vcores=live_part.vcores if live_part else None,
            draft_vcores=draft_part.vcores if draft_part else None,
            delta_vcores=(
                draft_part.vcores - live_part.vcores
                if draft_part and live_part and draft_part.vcores is not None and live_part.vcores is not None
                else None
            ),
            live_state=live_q.state if live_q else None,
            draft_state=draft_q.state,
            live_resource_mode=live_mode,
            draft_resource_mode=draft_mode,
            live_user_limit_factor=live_ulf,
            draft_user_limit_factor=draft_ulf,
            live_ordering_policy=live_ordering,
            draft_ordering_policy=draft_ordering,
            live_max_applications=live_max_apps,
            draft_max_applications=draft_max_apps,
            live_max_am_resource_percent=live_max_am,
            draft_max_am_resource_percent=draft_max_am,
            live_max_parallel_apps=live_max_parallel,
            draft_max_parallel_apps=draft_max_parallel,
            live_max_application_lifetime=live_lifetime,
            draft_max_application_lifetime=draft_lifetime,
        ))

    has_changes = any(d.action != "unchanged" for d in diffs)

    # Проверяем изменение queue_mappings
    mappings_diff = None
    live_mappings = getattr(cluster, "queue_mappings", "")
    live_override = getattr(cluster, "queue_mappings_override", False)
    if body.queue_mappings is not None and body.queue_mappings.strip() != (live_mappings or "").strip():
        mappings_diff = {
            "live": live_mappings,
            "draft": body.queue_mappings,
            "override_live": live_override,
            "override_draft": body.queue_mappings_override if body.queue_mappings_override is not None else live_override,
        }
        has_changes = True
    elif body.queue_mappings_override is not None and body.queue_mappings_override != live_override:
        mappings_diff = {
            "live": live_mappings,
            "draft": body.queue_mappings or live_mappings,
            "override_live": live_override,
            "override_draft": body.queue_mappings_override,
        }
        has_changes = True

    return DraftDiffResponse(
        cluster_id=cluster_id,
        has_changes=has_changes,
        diffs=diffs,
        queue_mappings_diff=mappings_diff,
    )


@router.post("/{cluster_id}/generate-xml", response_model=GenerateXmlResponse)
async def generate_xml(
    cluster_id: str,
    body: GenerateXmlRequest,
    user: UserSession = Depends(get_current_user),
):
    """
    Генерирует capacity-scheduler.xml.
    Доступно: ТОЛЬКО admin.
    Writer получит HTTP 403 с указанием обратиться к администратору.
    """
    cluster = _find_cluster(cluster_id)
    check_cluster_permission(user, cluster, Role.ADMIN)

    base_xml: Optional[str] = None
    if settings.auth.mode == "mock":
        from app.services.mock_yarn import get_mock_capacity_scheduler_xml
        base_xml = get_mock_capacity_scheduler_xml(cluster)
    else:
        try:
            from app.services.yarn_client import YarnClient
            client = YarnClient(cluster)
            base_xml = await client.get_capacity_scheduler_xml(do_as=user.username)
        except Exception as e:
            logger.warning(f"Не удалось получить текущий capacity-scheduler.xml из YARN: {e}")

    if not base_xml:
        from app.services.mock_yarn import get_mock_capacity_scheduler_xml
        base_xml = get_mock_capacity_scheduler_xml(cluster)

    xml_content = generate_capacity_scheduler_xml(
        queues=body.queues,
        cluster=cluster,
        generated_by=user.username,
        comment=body.proposal_comment or "",
        resource_mode=body.resource_mode_override,
        queue_mappings=body.queue_mappings,
        queue_mappings_override=body.queue_mappings_override,
        base_xml=base_xml,
    )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    instructions = (
        "Инструкции по применению:\n"
        f"1. Скопируйте файл capacity-scheduler.xml на все RM узлы кластера '{cluster.name}'\n"
        f"   Путь: /etc/hadoop/conf/capacity-scheduler.xml\n"
        "2. Выполните на активном ResourceManager:\n"
        "   yarn rmadmin -refreshQueues\n"
        "3. Проверьте в YARN UI: http://<rm-host>:8088/cluster/scheduler\n"
    )

    return GenerateXmlResponse(
        cluster_id=cluster_id,
        filename=f"capacity-scheduler-{cluster_id}-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.xml",
        xml_content=xml_content,
        applied_by=user.username,
        generated_at=now,
        instructions=instructions,
    )
