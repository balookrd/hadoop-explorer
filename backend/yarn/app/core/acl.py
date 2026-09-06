from typing import List, Optional
from fastapi import HTTPException, status

from app.core.config import settings
from app.models.auth import UserSession, Role
from app.models.cluster import ClusterConfig


def check_ui_access(user: UserSession) -> bool:
    """
    Проверяет, имеет ли пользователь право доступа к Web UI.
    """
    if user.is_admin:
        return True

    ui_acl = settings.acl.ui_access

    if "*" in ui_acl.allowed_users or user.username in ui_acl.allowed_users:
        return True

    if "*" in ui_acl.allowed_groups:
        return True

    user_groups = set(user.groups)
    allowed_groups = set(ui_acl.allowed_groups)
    if bool(user_groups & allowed_groups):
        return True

    return False


def _check_match(username: str, user_groups: set, allowed_users: List[str], allowed_groups: List[str]) -> bool:
    if "*" in allowed_users or username in allowed_users:
        return True
    if "*" in allowed_groups:
        return True
    if bool(user_groups & set(allowed_groups)):
        return True
    return False


def resolve_cluster_role(user: UserSession, cluster: ClusterConfig) -> Optional[Role]:
    """
    Определяет роль пользователя для конкретного кластера:
    - None: доступа к кластеру нет
    - Role.READER: только чтение
    - Role.WRITER: моделирование/редактирование, создание очередей, сохранение черновика
    - Role.ADMIN: полный доступ, применение и генерация конфигурационного файла
    """
    # 1. Проверка общего доступа к кластеру
    user_groups = set(user.groups)
    cluster_acl = cluster.acl

    has_cluster_access = (
        user.is_admin or
        _check_match(user.username, user_groups, cluster_acl.allowed_users, cluster_acl.allowed_groups)
    )
    if not has_cluster_access:
        return None

    if user.is_admin:
        return Role.ADMIN

    # 2. Проверка по ролям кластера
    # Admin
    if _check_match(user.username, user_groups, cluster_acl.roles.admin.users, cluster_acl.roles.admin.groups):
        return Role.ADMIN

    # Writer
    if _check_match(user.username, user_groups, cluster_acl.roles.writer.users, cluster_acl.roles.writer.groups):
        return Role.WRITER

    # Reader
    if _check_match(user.username, user_groups, cluster_acl.roles.reader.users, cluster_acl.roles.reader.groups):
        return Role.READER

    # 3. Fallback на глобальные роли из settings.acl.roles
    global_roles = settings.acl.roles
    if _check_match(user.username, user_groups, global_roles.admin.users, global_roles.admin.groups):
        return Role.ADMIN

    if _check_match(user.username, user_groups, global_roles.writer.users, global_roles.writer.groups):
        return Role.WRITER

    if _check_match(user.username, user_groups, global_roles.reader.users, global_roles.reader.groups):
        return Role.READER

    return Role.READER


def check_cluster_permission(user: UserSession, cluster: ClusterConfig, min_role: Role) -> Role:
    """
    Проверяет, что у пользователя роль в кластере не ниже min_role.
    """
    role = resolve_cluster_role(user, cluster)
    if role is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"У вас нет прав доступа к кластеру '{cluster.name}' ({cluster.id})"
        )

    role_priority = {
        Role.READER: 1,
        Role.WRITER: 2,
        Role.ADMIN: 3
    }

    if role_priority[role] < role_priority[min_role]:
        if min_role == Role.ADMIN:
            msg = (
                "Применение изменений и генерация конфигурации capacity-scheduler.xml "
                "разрешены только Администраторам (Admin). "
                "Как редактор (Writer) вы можете сохранить черновик и отправить его на ревью."
            )
        elif min_role == Role.WRITER:
            msg = "Редактирование очередей и создание черновиков требуют прав Редактора (Writer) или Администратора (Admin)."
        else:
            msg = "Недостаточно прав для выполнения данной операции."

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=msg
        )

    return role
