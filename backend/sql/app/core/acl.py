from typing import List
from app.core.config import settings, ClusterConfig
from app.core.security import UserSession

def check_ui_access(user: UserSession) -> bool:
    """
    Проверяет, имеет ли пользователь право доступа к Web UI.
    """
    if user.is_admin:
        return True

    ui_acl = settings.acl.ui_access

    # Проверка списка разрешенных пользователей
    if "*" in ui_acl.allowed_users or user.username in ui_acl.allowed_users:
        return True

    # Проверка групп пользователя
    if "*" in ui_acl.allowed_groups:
        return True

    user_groups = set(user.groups)
    allowed_groups = set(ui_acl.allowed_groups)
    if bool(user_groups & allowed_groups):
        return True

    return False

def check_cluster_access(user: UserSession, cluster: ClusterConfig) -> bool:
    """
    Проверяет, разрешен ли пользователю доступ к конкретному кластеру Trino/Hive.
    """
    if user.is_admin:
        return True

    acl = cluster.acl

    # Проверка прямых прав пользователя
    if "*" in acl.allowed_users or user.username in acl.allowed_users:
        return True

    # Проверка членства в группах
    if "*" in acl.allowed_groups:
        return True

    user_groups = set(user.groups)
    allowed_groups = set(acl.allowed_groups)
    if bool(user_groups & allowed_groups):
        return True

    return False

def filter_allowed_clusters(user: UserSession) -> List[ClusterConfig]:
    """
    Возвращает список только тех кластеров, к которым у пользователя есть доступ.
    """
    allowed = []
    for cluster in settings.clusters:
        if check_cluster_access(user, cluster):
            allowed.append(cluster)
    return allowed
