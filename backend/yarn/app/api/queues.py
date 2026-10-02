import logging
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status

from app.core.config import settings
from app.core.security import get_current_user
from app.core.acl import check_cluster_permission, resolve_cluster_role
from backend.common.models.auth import UserSession, Role
from app.models.cluster import ClusterConfig
from app.models.yarn import (
    QueueTreeResponse,
    DraftValidateRequest,
    DraftValidateResponse,
    GenerateXmlRequest,
    GenerateXmlResponse,
    DraftDiffResponse,
    DiffItem,
    DirectDeployXmlRequest,
    DirectDeployXmlResponse,
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
    default_partition = body.selected_partition or cluster.default_partition or "DEFAULT"

    for draft_q in body.queues:
        draft_paths.add(draft_q.path)
        live_q = live_map.get(draft_q.path)

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

        live_labels = getattr(live_q, "accessible_node_labels", None) if live_q else None
        draft_labels = draft_q.accessible_node_labels
        live_default_label = getattr(live_q, "default_node_label_expression", None) if live_q else None
        draft_default_label = draft_q.default_node_label_expression

        def build_diff_item(part_name: str, action: str) -> DiffItem:
            d_part = draft_q.partitions.get(part_name) if draft_q.partitions else None
            l_part = live_q.partitions.get(part_name) if (live_q and live_q.partitions) else None

            delta_cap = round(d_part.capacity - l_part.capacity, 2) if (d_part and l_part) else None
            delta_max_cap = round(d_part.max_capacity - l_part.max_capacity, 2) if (d_part and l_part) else None
            delta_mem = (
                d_part.memory_mb - l_part.memory_mb
                if d_part and l_part and d_part.memory_mb is not None and l_part.memory_mb is not None
                else None
            )
            delta_vcores = (
                d_part.vcores - l_part.vcores
                if d_part and l_part and d_part.vcores is not None and l_part.vcores is not None
                else None
            )

            return DiffItem(
                path=draft_q.path,
                name=draft_q.name,
                parent_path=draft_q.parent_path,
                partition=part_name,
                action=action,
                live_capacity=l_part.capacity if l_part else None,
                draft_capacity=d_part.capacity if d_part else None,
                delta_capacity=delta_cap,
                live_max_capacity=l_part.max_capacity if l_part else None,
                draft_max_capacity=d_part.max_capacity if d_part else None,
                delta_max_capacity=delta_max_cap,
                live_memory_mb=l_part.memory_mb if l_part else None,
                draft_memory_mb=d_part.memory_mb if d_part else None,
                delta_memory_mb=delta_mem,
                live_vcores=l_part.vcores if l_part else None,
                draft_vcores=d_part.vcores if d_part else None,
                delta_vcores=delta_vcores,
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
                live_accessible_node_labels=live_labels,
                draft_accessible_node_labels=draft_labels,
                live_default_node_label_expression=live_default_label,
                draft_default_node_label_expression=draft_default_label,
            )

        if draft_q.action == "create":
            parts = list(draft_q.partitions.keys()) if draft_q.partitions else [default_partition]
            for p in parts:
                diffs.append(build_diff_item(p, "created"))
        elif draft_q.action == "delete":
            diffs.append(build_diff_item(default_partition, "deleted"))
        elif live_q:
            # Проверяем изменения общих свойств очереди
            queue_props_changed = False
            if live_q.state != draft_q.state:
                queue_props_changed = True
            if draft_q.resource_mode and live_mode and draft_q.resource_mode != live_mode:
                queue_props_changed = True
            if draft_ulf is not None and live_ulf is not None and abs(draft_ulf - live_ulf) > 0.001:
                queue_props_changed = True
            if draft_ordering and live_ordering and draft_ordering.lower() != live_ordering.lower():
                queue_props_changed = True
            if draft_max_apps is not None and draft_max_apps != live_max_apps:
                queue_props_changed = True
            if draft_max_am is not None and (live_max_am is None or abs(draft_max_am - live_max_am) > 0.001):
                queue_props_changed = True
            if draft_max_parallel is not None and draft_max_parallel != live_max_parallel:
                queue_props_changed = True
            if draft_lifetime is not None and draft_lifetime != live_lifetime:
                queue_props_changed = True

            # Проверка Node Labels / Partitioning
            if draft_labels is not None:
                sorted_draft = sorted(draft_labels)
                sorted_live = sorted(live_labels or [])
                if sorted_draft != sorted_live:
                    queue_props_changed = True
            if draft_default_label is not None or live_default_label is not None:
                if (draft_default_label or "").strip() != (live_default_label or "").strip():
                    queue_props_changed = True

            # Проверяем изменения по конкретным разделам
            all_parts = set(draft_q.partitions.keys()) | (set(live_q.partitions.keys()) if live_q.partitions else set())
            if not all_parts:
                all_parts = {default_partition}

            changed_parts = []
            for p in all_parts:
                d_p = draft_q.partitions.get(p) if draft_q.partitions else None
                l_p = live_q.partitions.get(p) if live_q.partitions else None
                p_changed = False
                if (d_p is None) != (l_p is None):
                    p_changed = True
                elif d_p and l_p:
                    if abs(d_p.capacity - l_p.capacity) > 0.01 or abs(d_p.max_capacity - l_p.max_capacity) > 0.01:
                        p_changed = True
                    elif d_p.memory_mb is not None and l_p.memory_mb is not None and d_p.memory_mb != l_p.memory_mb:
                        p_changed = True
                    elif d_p.vcores is not None and l_p.vcores is not None and d_p.vcores != l_p.vcores:
                        p_changed = True
                if p_changed:
                    changed_parts.append(p)

            if changed_parts:
                for p in changed_parts:
                    diffs.append(build_diff_item(p, "modified"))
            elif queue_props_changed:
                diffs.append(build_diff_item(default_partition, "modified"))
            else:
                diffs.append(build_diff_item(default_partition, "unchanged"))
        else:
            diffs.append(build_diff_item(default_partition, "created"))

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
            "override_draft": body.queue_mappings_override
            if body.queue_mappings_override is not None
            else live_override,
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


@router.post("/{cluster_id}/deploy-xml", response_model=DirectDeployXmlResponse)
async def deploy_cluster_xml(
    cluster_id: str,
    body: DirectDeployXmlRequest,
    user: UserSession = Depends(get_current_user),
):
    """
    Прямое развертывание и применение capacity-scheduler.xml на кластере через AWX.
    Доступно: ТОЛЬКО admin.
    """
    cluster = _find_cluster(cluster_id)
    check_cluster_permission(user, cluster, Role.ADMIN)

    if not body.xml_content.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Тело XML не может быть пустым",
        )

    job_template_id = None
    if cluster.awx and cluster.awx.job_template_id:
        job_template_id = cluster.awx.job_template_id
    elif settings.awx.default_job_template_id:
        job_template_id = settings.awx.default_job_template_id

    if not job_template_id:
        if settings.auth.mode == "mock" or (settings.awx.enabled is False and not settings.awx.token):
            job_template_id = 1
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"AWX Job Template ID не настроен для кластера '{cluster.name}'",
            )

    from app.services.awx_client import AwxClient, AwxClientError, AwxJobFailedError

    awx_client = AwxClient()
    try:
        job_id = await awx_client.launch_job(
            job_template_id=job_template_id,
            xml_content=body.xml_content,
            applied_by=user.username,
            cluster_id=cluster.id,
            change_request_id=None,
        )
        result = await awx_client.wait_for_job(job_id)
        now_str = datetime.now(timezone.utc).isoformat()
        return DirectDeployXmlResponse(
            cluster_id=cluster_id,
            awx_job_id=job_id,
            status="SUCCESS",
            message=f"XML успешно применен на кластере '{cluster.name}' через AWX",
            deployed_at=now_str,
            stdout=result.get("stdout"),
        )
    except AwxJobFailedError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Сбой выполнения AWX Job #{e.job_id} ({e.status}): {e.stdout[-300:] if e.stdout else 'Без лога'}",
        )
    except AwxClientError as e:
        raise HTTPException(status_code=502, detail=f"Ошибка взаимодействия с AWX: {e}")
