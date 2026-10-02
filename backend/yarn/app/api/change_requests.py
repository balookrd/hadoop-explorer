import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.core.security import get_current_user
from app.core.rate_limiter import get_client_ip
from app.core.config import settings
from app.core.acl import check_cluster_permission, resolve_cluster_role
from backend.common.models.auth import UserSession, Role
from app.models.yarn import DiffItem, QueueNode, QueueDraftItem
from app.models.change_requests import (
    ChangeRequestCreate,
    ChangeRequestReview,
    ChangeRequestResponse,
    ChangeRequestSummary,
    DeployResponse,
)
from app.services.storage import storage_service
from app.services.xml_generator import generate_capacity_scheduler_xml
from app.api.queues import _find_cluster

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/change-requests", tags=["change-requests"])


def _flatten_tree(node: QueueNode) -> List[QueueNode]:
    result = [node]
    for child in node.children:
        result.extend(_flatten_tree(child))
    return result


async def _get_live_queues(cluster, username: str) -> List[QueueNode]:
    if settings.auth.mode == "mock":
        from app.services.mock_yarn import get_mock_queue_tree

        root = get_mock_queue_tree(cluster)
    else:
        from app.services.yarn_client import YarnClient

        client = YarnClient(cluster)
        root, _ = await client.get_queue_tree(username)
    return _flatten_tree(root)


@router.get("", response_model=List[ChangeRequestSummary])
@router.get("/", response_model=List[ChangeRequestSummary])
async def list_change_requests(
    cluster_id: Optional[str] = Query(None, description="Фильтр по ID кластера"),
    status_filter: Optional[str] = Query(None, alias="status", description="Фильтр по статусу"),
    current_user: UserSession = Depends(get_current_user),
):
    """Получить список заявок на изменение очередей для доступных пользователю кластеров."""
    if cluster_id:
        cluster = _find_cluster(cluster_id)
        check_cluster_permission(current_user, cluster, Role.READER)
        return storage_service.list_change_requests(cluster_id=cluster_id, status=status_filter)

    all_crs = storage_service.list_change_requests(status=status_filter)
    accessible_cluster_ids = {c.id for c in settings.clusters if resolve_cluster_role(current_user, c) is not None}
    return [cr for cr in all_crs if cr.cluster_id in accessible_cluster_ids]


@router.get("/pending-count")
async def get_pending_count(
    cluster_id: Optional[str] = Query(None),
    current_user: UserSession = Depends(get_current_user),
):
    """Количество заявок, ожидающих согласования (только для доступных кластеров)."""
    if cluster_id:
        cluster = _find_cluster(cluster_id)
        check_cluster_permission(current_user, cluster, Role.READER)
        count = storage_service.count_pending(cluster_id=cluster_id)
    else:
        all_crs = storage_service.list_change_requests(status="SUBMITTED")
        accessible_cluster_ids = {c.id for c in settings.clusters if resolve_cluster_role(current_user, c) is not None}
        count = sum(1 for cr in all_crs if cr.cluster_id in accessible_cluster_ids)
    return {"pending_count": count}


@router.post("", response_model=ChangeRequestResponse, status_code=status.HTTP_201_CREATED)
@router.post("/", response_model=ChangeRequestResponse, status_code=status.HTTP_201_CREATED)
async def create_change_request(
    request: ChangeRequestCreate,
    http_request: Request,
    current_user: UserSession = Depends(get_current_user),
):
    """Создать заявку на изменение конфигурации очередей (роли writer и admin)."""
    cluster = _find_cluster(request.cluster_id)
    check_cluster_permission(current_user, cluster, Role.WRITER)

    partition = cluster.default_partition or "DEFAULT"
    live_nodes = await _get_live_queues(cluster, current_user.username)
    live_map = {n.path: n for n in live_nodes}

    diffs: List[DiffItem] = []
    default_partition = cluster.default_partition or "DEFAULT"

    for draft_q in request.changes:
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

        def build_cr_diff_item(part_name: str, action: str) -> DiffItem:
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
                diffs.append(build_cr_diff_item(p, "created"))
        elif draft_q.action == "delete":
            diffs.append(build_cr_diff_item(default_partition, "deleted"))
        elif live_q:
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

            if draft_labels is not None:
                if sorted(draft_labels) != sorted(live_labels or []):
                    queue_props_changed = True
            if draft_default_label is not None or live_default_label is not None:
                if (draft_default_label or "").strip() != (live_default_label or "").strip():
                    queue_props_changed = True

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
                    diffs.append(build_cr_diff_item(p, "modified"))
            elif queue_props_changed:
                diffs.append(build_cr_diff_item(default_partition, "modified"))
            else:
                diffs.append(build_cr_diff_item(default_partition, "unchanged"))
        else:
            diffs.append(build_cr_diff_item(default_partition, "created"))

    cr_id = storage_service.create_change_request(
        cluster_id=request.cluster_id,
        title=request.title,
        description=request.description or "",
        author=current_user.username,
        changes=request.changes,
        diffs=diffs,
    )

    created = storage_service.get_change_request(cr_id)
    if not created:
        raise HTTPException(status_code=500, detail="Ошибка при создании заявки")

    from app.core.audit import audit_log

    audit_log(
        action="CR_CREATED",
        username=current_user.username,
        client_ip=get_client_ip(http_request),
        details={
            "cr_id": cr_id,
            "cluster_id": request.cluster_id,
            "title": request.title,
            "changes_count": len(request.changes),
        },
        status="SUCCESS",
    )
    return created


@router.get("/{cr_id}", response_model=ChangeRequestResponse)
async def get_change_request(
    cr_id: int,
    current_user: UserSession = Depends(get_current_user),
):
    """Получить подробную информацию о заявке по ID (требуются права Reader в кластере заявки)."""
    cr = storage_service.get_change_request(cr_id)
    if not cr:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Заявка #{cr_id} не найдена",
        )
    cluster = _find_cluster(cr.cluster_id)
    check_cluster_permission(current_user, cluster, Role.READER)
    return cr


@router.post("/{cr_id}/approve", response_model=ChangeRequestResponse)
async def approve_change_request(
    cr_id: int,
    review: ChangeRequestReview,
    http_request: Request,
    current_user: UserSession = Depends(get_current_user),
):
    """Одобрить заявку на изменение и сгенерировать XML (только admin)."""
    cr = storage_service.get_change_request(cr_id)
    if not cr:
        raise HTTPException(status_code=404, detail=f"Заявка #{cr_id} не найдена")

    cluster = _find_cluster(cr.cluster_id)
    check_cluster_permission(current_user, cluster, Role.ADMIN)

    # Принцип Four-Eyes (разделение обязанностей): автор заявки не может самостоятельно одобрить свой запрос (если включено в настройках)
    if settings.acl.enforce_four_eyes and cr.author == current_user.username:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Принцип разделения обязанностей (Four-Eyes): автор заявки не может самостоятельно одобрить свой запрос",
        )

    if cr.status != "SUBMITTED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Нельзя одобрить заявку в статусе '{cr.status}' (ожидается SUBMITTED)",
        )

    live_nodes = await _get_live_queues(cluster, current_user.username)
    queue_map = {}
    for n in live_nodes:
        queue_map[n.path] = QueueDraftItem(
            path=n.path,
            name=n.name,
            parent_path=n.parent_path,
            action="modify",
            is_leaf=n.is_leaf,
            state=n.state,
            partitions=n.partitions,
        )
    for draft_q in cr.changes:
        if draft_q.action == "delete":
            queue_map.pop(draft_q.path, None)
        else:
            queue_map[draft_q.path] = draft_q

    all_queues = list(queue_map.values())

    base_xml: Optional[str] = None
    if settings.auth.mode == "mock":
        from app.services.mock_yarn import get_mock_capacity_scheduler_xml

        base_xml = get_mock_capacity_scheduler_xml(cluster)
    else:
        try:
            from app.services.yarn_client import YarnClient

            client = YarnClient(cluster)
            base_xml = await client.get_capacity_scheduler_xml(do_as=current_user.username)
        except Exception as e:
            logger.warning(f"Не удалось получить текущий capacity-scheduler.xml из YARN: {e}")

    if not base_xml:
        from app.services.mock_yarn import get_mock_capacity_scheduler_xml

        base_xml = get_mock_capacity_scheduler_xml(cluster)

    xml_content = generate_capacity_scheduler_xml(
        queues=all_queues,
        cluster=cluster,
        generated_by=f"{cr.author} (Approved by {current_user.username})",
        comment=review.comment or f"Approved Change Request #{cr.id}: {cr.title}",
        resource_mode=cluster.resource_mode,
        base_xml=base_xml,
    )

    success = storage_service.approve_change_request(
        cr_id=cr_id,
        reviewer=current_user.username,
        comment=review.comment or "",
        xml_content=xml_content,
    )

    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при одобрении заявки")

    from app.core.audit import audit_log

    audit_log(
        action="CR_APPROVED",
        username=current_user.username,
        client_ip=get_client_ip(http_request),
        details={"cr_id": cr_id, "cluster_id": cr.cluster_id, "author": cr.author, "comment": review.comment or ""},
        status="SUCCESS",
    )

    return storage_service.get_change_request(cr_id)


@router.post("/{cr_id}/reject", response_model=ChangeRequestResponse)
async def reject_change_request(
    cr_id: int,
    review: ChangeRequestReview,
    http_request: Request,
    current_user: UserSession = Depends(get_current_user),
):
    """Отклонить заявку на изменение (только admin)."""
    cr = storage_service.get_change_request(cr_id)
    if not cr:
        raise HTTPException(status_code=404, detail=f"Заявка #{cr_id} не найдена")

    cluster = _find_cluster(cr.cluster_id)
    check_cluster_permission(current_user, cluster, Role.ADMIN)

    if cr.status != "SUBMITTED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Нельзя отклонить заявку в статусе '{cr.status}'",
        )

    success = storage_service.reject_change_request(
        cr_id=cr_id,
        reviewer=current_user.username,
        comment=review.comment or "Отклонено администратором",
    )

    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при отклонении заявки")

    from app.core.audit import audit_log

    audit_log(
        action="CR_REJECTED",
        username=current_user.username,
        client_ip=get_client_ip(http_request),
        details={"cr_id": cr_id, "cluster_id": cr.cluster_id, "author": cr.author, "comment": review.comment or ""},
        status="SUCCESS",
    )

    return storage_service.get_change_request(cr_id)


@router.post("/{cr_id}/cancel", response_model=ChangeRequestResponse)
async def cancel_change_request(
    cr_id: int,
    http_request: Request,
    current_user: UserSession = Depends(get_current_user),
):
    """Отозвать заявку (доступно автору или администратору)."""
    cr = storage_service.get_change_request(cr_id)
    if not cr:
        raise HTTPException(status_code=404, detail=f"Заявка #{cr_id} не найдена")

    if cr.status != "SUBMITTED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Нельзя отозвать заявку в статусе '{cr.status}'",
        )

    cluster = _find_cluster(cr.cluster_id)
    role = resolve_cluster_role(current_user, cluster)
    if cr.author != current_user.username and role != Role.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Вы можете отзывать только свои заявки",
        )

    success = storage_service.cancel_change_request(cr_id=cr_id, author=cr.author)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при отзыве заявки")

    from app.core.audit import audit_log

    audit_log(
        action="CR_CANCELLED",
        username=current_user.username,
        client_ip=get_client_ip(http_request),
        details={"cr_id": cr_id, "cluster_id": cr.cluster_id, "author": cr.author},
        status="SUCCESS",
    )

    return storage_service.get_change_request(cr_id)


@router.get("/{cr_id}/preview-xml")
async def preview_change_request_xml(
    cr_id: int,
    current_user: UserSession = Depends(get_current_user),
):
    """Предпросмотр сгенерированного XML для заявки на согласование."""
    cr = storage_service.get_change_request(cr_id)
    if not cr:
        raise HTTPException(status_code=404, detail=f"Заявка #{cr_id} не найдена")

    cluster = _find_cluster(cr.cluster_id)
    check_cluster_permission(current_user, cluster, Role.READER)

    if cr.xml_content and cr.status == "APPROVED":
        return {
            "cr_id": cr.id,
            "title": cr.title,
            "filename": f"capacity-scheduler-{cr.cluster_id}.xml",
            "xml_content": cr.xml_content,
        }

    live_nodes = await _get_live_queues(cluster, current_user.username)
    queue_map = {}
    for n in live_nodes:
        queue_map[n.path] = QueueDraftItem(
            path=n.path,
            name=n.name,
            parent_path=n.parent_path,
            action="modify",
            is_leaf=n.is_leaf,
            state=n.state,
            partitions=n.partitions,
        )
    for draft_q in cr.changes:
        if draft_q.action == "delete":
            queue_map.pop(draft_q.path, None)
        else:
            queue_map[draft_q.path] = draft_q

    all_queues = list(queue_map.values())

    base_xml: Optional[str] = None
    if settings.auth.mode == "mock":
        from app.services.mock_yarn import get_mock_capacity_scheduler_xml

        base_xml = get_mock_capacity_scheduler_xml(cluster)
    else:
        try:
            from app.services.yarn_client import YarnClient

            client = YarnClient(cluster)
            base_xml = await client.get_capacity_scheduler_xml(do_as=current_user.username)
        except Exception as e:
            logger.warning(f"Не удалось получить текущий capacity-scheduler.xml из YARN: {e}")

    if not base_xml:
        from app.services.mock_yarn import get_mock_capacity_scheduler_xml

        base_xml = get_mock_capacity_scheduler_xml(cluster)

    xml_content = generate_capacity_scheduler_xml(
        queues=all_queues,
        cluster=cluster,
        generated_by=f"{cr.author} (Preview by {current_user.username})",
        comment=f"Preview Change Request #{cr.id}: {cr.title}",
        resource_mode=cluster.resource_mode,
        base_xml=base_xml,
    )

    return {
        "cr_id": cr.id,
        "title": cr.title,
        "filename": f"capacity-scheduler-{cr.cluster_id}.xml",
        "xml_content": xml_content,
    }


@router.post("/{cr_id}/deploy", response_model=DeployResponse)
async def deploy_change_request(
    cr_id: int,
    http_request: Request,
    wait: bool = Query(default=True, description="Ожидать ли завершения выполнения задачи в AWX"),
    current_user: UserSession = Depends(get_current_user),
):
    """
    Развертывание и применение конфигурации approved заявки на кластере через AWX.
    Доступно: только ADMIN в кластере заявки.
    """
    cr = storage_service.get_change_request(cr_id)
    if not cr:
        raise HTTPException(status_code=404, detail=f"Заявка #{cr_id} не найдена")

    cluster = _find_cluster(cr.cluster_id)
    check_cluster_permission(current_user, cluster, Role.ADMIN)

    if cr.status != "APPROVED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Нельзя развернуть заявку со статусом '{cr.status}' (ожидается APPROVED)",
        )

    # Определение job_template_id
    job_template_id = None
    if cluster.awx and cluster.awx.job_template_id:
        job_template_id = cluster.awx.job_template_id
    elif settings.awx.default_job_template_id:
        job_template_id = settings.awx.default_job_template_id

    # В mock-режиме, если ID не задан, используем дефолтный 1
    if not job_template_id:
        if settings.auth.mode == "mock" or (settings.awx.enabled is False and not settings.awx.token):
            job_template_id = 1
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"AWX Job Template ID не настроен для кластера '{cluster.name}' (проверьте config.yaml)",
            )

    xml_content = cr.xml_content
    if not xml_content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="В заявке отсутствует сгенерированный capacity-scheduler.xml",
        )

    from app.services.awx_client import AwxClient, AwxClientError, AwxJobFailedError
    from app.core.audit import audit_log
    from datetime import datetime, timezone

    awx_client = AwxClient()

    try:
        job_id = await awx_client.launch_job(
            job_template_id=job_template_id,
            xml_content=xml_content,
            applied_by=current_user.username,
            cluster_id=cluster.id,
            change_request_id=cr.id,
        )
    except AwxClientError as e:
        logger.error(f"Ошибка запуска AWX джоба для CR #{cr_id}: {e}")
        storage_service.update_deployment_status(
            cr_id=cr.id,
            deployment_status="FAILED",
            deployment_error=str(e),
        )
        raise HTTPException(status_code=502, detail=f"Ошибка запуска задачи в AWX: {e}")

    storage_service.update_deployment_status(
        cr_id=cr.id,
        deployment_status="DEPLOYING",
        awx_job_id=job_id,
    )

    if not wait:
        return DeployResponse(
            cr_id=cr.id,
            cluster_id=cluster.id,
            awx_job_id=job_id,
            status="DEPLOYING",
            message=f"Задача #{job_id} запущена в AWX",
        )

    now_str = datetime.now(timezone.utc).isoformat()
    try:
        result = await awx_client.wait_for_job(job_id)
        storage_service.update_deployment_status(
            cr_id=cr.id,
            deployment_status="SUCCESS",
            awx_job_id=job_id,
            deployed_at=now_str,
        )
        audit_log(
            action="CR_DEPLOYED",
            username=current_user.username,
            client_ip=get_client_ip(http_request),
            details={"cr_id": cr.id, "cluster_id": cluster.id, "awx_job_id": job_id},
            status="SUCCESS",
        )
        return DeployResponse(
            cr_id=cr.id,
            cluster_id=cluster.id,
            awx_job_id=job_id,
            status="SUCCESS",
            message="Конфигурация успешно развернута и применена на кластере через AWX",
            deployed_at=now_str,
            stdout=result.get("stdout"),
        )
    except AwxJobFailedError as e:
        storage_service.update_deployment_status(
            cr_id=cr.id,
            deployment_status="FAILED",
            awx_job_id=job_id,
            deployment_error=f"Статус {e.status}: {e.stdout[-300:] if e.stdout else 'Без лога'}",
        )
        audit_log(
            action="CR_DEPLOY_FAILED",
            username=current_user.username,
            client_ip=get_client_ip(http_request),
            details={"cr_id": cr.id, "cluster_id": cluster.id, "awx_job_id": job_id, "error": e.status},
            status="FAILURE",
        )
        return DeployResponse(
            cr_id=cr.id,
            cluster_id=cluster.id,
            awx_job_id=job_id,
            status="FAILED",
            message=f"Сбой выполнения плейбука в AWX: {e.status}",
            stdout=e.stdout,
        )
    except AwxClientError as e:
        storage_service.update_deployment_status(
            cr_id=cr.id,
            deployment_status="FAILED",
            awx_job_id=job_id,
            deployment_error=str(e),
        )
        raise HTTPException(status_code=504, detail=f"Таймаут или ошибка ожидания AWX: {e}")


@router.get("/{cr_id}/deploy-status", response_model=DeployResponse)
async def get_deploy_status(
    cr_id: int,
    current_user: UserSession = Depends(get_current_user),
):
    """
    Получение актуального статуса выполнения задачи деплоя в AWX.
    Доступно: READER и выше.
    """
    cr = storage_service.get_change_request(cr_id)
    if not cr:
        raise HTTPException(status_code=404, detail=f"Заявка #{cr_id} не найдена")

    cluster = _find_cluster(cr.cluster_id)
    check_cluster_permission(current_user, cluster, Role.READER)

    if not cr.awx_job_id:
        return DeployResponse(
            cr_id=cr.id,
            cluster_id=cluster.id,
            awx_job_id=0,
            status=cr.deployment_status or "NOT_STARTED",
            message="Задача деплоя еще не запускалась",
            deployed_at=cr.deployed_at,
        )

    from app.services.awx_client import AwxClient

    awx_client = AwxClient()
    job_info = await awx_client.get_job_status(cr.awx_job_id)
    stdout = await awx_client.get_job_stdout(cr.awx_job_id)
    awx_status = job_info.get("status", "unknown")

    # Синхронизация статуса в БД при необходимости
    mapped_status = "DEPLOYING"
    if awx_status == "successful":
        mapped_status = "SUCCESS"
    elif awx_status in ("failed", "error", "canceled"):
        mapped_status = "FAILED"

    if mapped_status != cr.deployment_status and mapped_status != "DEPLOYING":
        from datetime import datetime, timezone

        storage_service.update_deployment_status(
            cr_id=cr.id,
            deployment_status=mapped_status,
            awx_job_id=cr.awx_job_id,
            deployed_at=datetime.now(timezone.utc).isoformat() if mapped_status == "SUCCESS" else None,
            deployment_error=None if mapped_status == "SUCCESS" else f"AWX status: {awx_status}",
        )

    return DeployResponse(
        cr_id=cr.id,
        cluster_id=cluster.id,
        awx_job_id=cr.awx_job_id,
        status=mapped_status,
        message=f"AWX job #{cr.awx_job_id} статус: {awx_status}",
        deployed_at=cr.deployed_at,
        stdout=stdout,
    )
