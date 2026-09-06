import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.core.security import get_current_user
from app.core.rate_limiter import get_client_ip
from app.core.config import settings
from app.core.acl import check_cluster_permission, resolve_cluster_role
from app.models.auth import UserSession, Role
from app.models.yarn import DiffItem, QueueNode, QueueDraftItem
from app.models.change_requests import (
    ChangeRequestCreate,
    ChangeRequestReview,
    ChangeRequestResponse,
    ChangeRequestSummary,
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
    accessible_cluster_ids = {
        c.id for c in settings.clusters
        if resolve_cluster_role(current_user, c) is not None
    }
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
        accessible_cluster_ids = {
            c.id for c in settings.clusters
            if resolve_cluster_role(current_user, c) is not None
        }
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
    for draft_q in request.changes:
        live_q = live_map.get(draft_q.path)
        draft_part = draft_q.partitions.get(partition)
        live_part = live_q.partitions.get(partition) if live_q else None

        if draft_q.action == "delete":
            action = "deleted"
        elif live_q:
            has_changes = False
            if live_part and draft_part:
                if (
                    abs(live_part.capacity - draft_part.capacity) > 0.01
                    or abs(live_part.max_capacity - draft_part.max_capacity) > 0.01
                ):
                    has_changes = True
            if live_q.state != draft_q.state:
                has_changes = True
            action = "modified" if has_changes else "unchanged"
        else:
            action = "created"

        diffs.append(
            DiffItem(
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
            )
        )

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
        details={"cr_id": cr_id, "cluster_id": request.cluster_id, "title": request.title, "changes_count": len(request.changes)},
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

    # Принцип Four-Eyes (разделение обязанностей): автор заявки не может самостоятельно одобрить свой запрос
    if cr.author == current_user.username:
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
