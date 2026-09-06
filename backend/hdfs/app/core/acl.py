from typing import List
from app.core.config import settings
from app.models.cluster import ClusterConfig, ClusterPublicInfo


def is_global_admin(username: str, user_groups: List[str]) -> bool:
    admin_groups = set(g.lower() for g in settings.acl.admin_groups)
    user_groups_set = set(g.lower() for g in user_groups)
    return bool(admin_groups & user_groups_set)


def can_access_ui(username: str, user_groups: List[str]) -> bool:
    if settings.acl.allow_all_authenticated:
        return True

    if is_global_admin(username, user_groups):
        return True

    # Проверка конкретного имени пользователя
    allowed_users = set(u.lower() for u in settings.acl.allowed_users)
    if username.lower() in allowed_users:
        return True

    # Проверка групп пользователя
    allowed_groups = set(g.lower() for g in settings.acl.allowed_groups)
    user_groups_set = set(g.lower() for g in user_groups)
    return bool(allowed_groups & user_groups_set)


def can_access_cluster(cluster: ClusterConfig, username: str, user_groups: List[str]) -> bool:
    # Глобальный админ всегда имеет доступ
    if is_global_admin(username, user_groups):
        return True

    acl = cluster.acl

    # Если в ACL кластера не заданы ограничения, разрешаем доступ
    if not acl.allowed_groups and not acl.allowed_users:
        return True

    # Проверка конкретного имени пользователя
    if username.lower() in [u.lower() for u in acl.allowed_users]:
        return True

    # Проверка групп кластера
    cluster_groups = set(g.lower() for g in acl.allowed_groups)
    user_groups_set = set(g.lower() for g in user_groups)
    if cluster_groups & user_groups_set:
        return True

    # Проверка администраторов кластера
    cluster_admins = set(g.lower() for g in acl.admin_groups)
    return bool(cluster_admins & user_groups_set)


def is_cluster_read_only(cluster: ClusterConfig, username: str, user_groups: List[str]) -> bool:
    if is_global_admin(username, user_groups):
        return False

    acl = cluster.acl
    cluster_admins = set(g.lower() for g in acl.admin_groups)
    user_groups_set = set(g.lower() for g in user_groups)
    if cluster_admins & user_groups_set:
        return False

    read_only_groups = set(g.lower() for g in acl.read_only_groups)
    return bool(read_only_groups & user_groups_set)


def is_cluster_admin(cluster: ClusterConfig, username: str, user_groups: List[str]) -> bool:
    if is_global_admin(username, user_groups):
        return True
    cluster_admins = set(g.lower() for g in cluster.acl.admin_groups)
    user_groups_set = set(g.lower() for g in user_groups)
    return bool(cluster_admins & user_groups_set)


def get_visible_clusters(clusters: List[ClusterConfig], username: str, user_groups: List[str]) -> List[ClusterPublicInfo]:
    visible = []
    for c in clusters:
        if can_access_cluster(c, username, user_groups):
            default_path = c.default_path.replace("{username}", username)
            visible.append(
                ClusterPublicInfo(
                    id=c.id,
                    name=c.name,
                    description=c.description,
                    default_path=default_path,
                    is_read_only=is_cluster_read_only(c, username, user_groups),
                    is_admin=is_cluster_admin(c, username, user_groups)
                )
            )
    return visible
