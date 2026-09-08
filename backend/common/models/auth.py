from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class Role(str, Enum):
    READER = "reader"
    WRITER = "writer"
    ADMIN = "admin"


DEFAULT_ADMIN_GROUPS = {
    "hadoop-admins",
    "admins",
    "data-platform-admins",
    "platform-admins",
    "superusers",
}
DEFAULT_WRITER_GROUPS = {
    "data-engineers",
    "engineers",
    "spark-users",
    "etl-developers",
}


def resolve_system_role(
    username: str,
    groups: Optional[List[str]] = None,
    settings: Optional[object] = None,
) -> tuple[Role, bool]:
    """
    Централизованно определяет (system_role, is_admin) по имени пользователя, его группам
    и конфигурационным настройкам сервиса.
    """
    user_groups = set(g.lower() for g in (groups or []))
    uname = (username or "").lower()

    admin_groups = set(DEFAULT_ADMIN_GROUPS)
    admin_users = set()
    writer_groups = set(DEFAULT_WRITER_GROUPS)
    writer_users = set()

    if settings:
        acl = getattr(settings, "acl", None)
        if acl:
            # YARN style: acl.roles.admin / writer
            roles = getattr(acl, "roles", None)
            if roles:
                adm = getattr(roles, "admin", None)
                if adm:
                    admin_groups.update(g.lower() for g in getattr(adm, "groups", []))
                    admin_users.update(u.lower() for u in getattr(adm, "users", []))
                wri = getattr(roles, "writer", None)
                if wri:
                    writer_groups.update(g.lower() for g in getattr(wri, "groups", []))
                    writer_users.update(u.lower() for u in getattr(wri, "users", []))

            # SQL / Spark style: acl.ui_access.admin_groups
            ui_acc = getattr(acl, "ui_access", None)
            if ui_acc:
                admin_groups.update(g.lower() for g in getattr(ui_acc, "admin_groups", []))

            # HDFS style: acl.admin_groups
            if hasattr(acl, "admin_groups") and isinstance(acl.admin_groups, (list, tuple, set)):
                admin_groups.update(g.lower() for g in acl.admin_groups)

    # 1. Проверка Admin
    if uname in admin_users or bool(user_groups & admin_groups):
        return Role.ADMIN, True

    # 2. Проверка Writer
    if uname in writer_users or bool(user_groups & writer_groups):
        return Role.WRITER, False

    # 3. Reader по умолчанию
    return Role.READER, False



class LoginRequest(BaseModel):
    username: str
    password: str


class UserInfo(BaseModel):
    username: str
    display_name: str
    email: Optional[str] = None
    groups: List[str] = Field(default_factory=list)
    is_admin: bool = False


class UserSession(BaseModel):
    username: str
    display_name: str
    email: Optional[str] = None
    groups: List[str] = Field(default_factory=list)
    auth_method: str = "mock"  # ldap, kerberos, mock
    is_admin: bool = False
    system_role: Role = Role.READER


class LoginResponse(BaseModel):
    success: bool
    user: UserInfo
    message: str = "Login successful"


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserSession
    success: bool = True
    message: str = "Авторизация успешна"


# AuthResponse синонимичен TokenResponse для обратной совместимости
AuthResponse = TokenResponse


class TokenPayload(BaseModel):
    sub: str  # username
    display_name: str
    email: Optional[str] = None
    groups: List[str] = Field(default_factory=list)
    exp: int
    jti: Optional[str] = None
    auth_method: Optional[str] = "jwt"
    is_admin: Optional[bool] = False
    system_role: Optional[str] = "reader"

