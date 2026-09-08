import secrets
from app.core.config import settings
from app.core.security import get_current_user
from app.core.ldap_auth import ldap_service
from app.core.acl import _check_match, check_ui_access
from app.core.rate_limiter import auth_rate_limiter
from app.services.storage import storage_service
from backend.common.core.kerberos import kerberos_manager
from backend.common.models.auth import Role, UserSession
from backend.common.api.auth_router import create_auth_router


def _resolve_global_role(username: str, groups: list) -> Role:
    """Определяет глобальную роль пользователя."""
    user_groups = set(groups)
    g = settings.acl.roles
    if _check_match(username, user_groups, g.admin.users, g.admin.groups):
        return Role.ADMIN
    if _check_match(username, user_groups, g.writer.users, g.writer.groups):
        return Role.WRITER
    return Role.READER


def _mock_authenticate(username: str, password: str):
    """Аутентификация через mock-пользователей."""
    for mock_user in settings.auth.mock_users:
        if secrets.compare_digest(mock_user.username, username):
            password_valid = False
            if mock_user.password_hash:
                import bcrypt

                try:
                    password_valid = bcrypt.checkpw(
                        password.encode("utf-8"),
                        mock_user.password_hash.encode("utf-8"),
                    )
                except Exception:
                    password_valid = False
            elif mock_user.password:
                password_valid = secrets.compare_digest(mock_user.password, password)

            if password_valid:
                role = _resolve_global_role(username, mock_user.groups)
                return UserSession(
                    username=mock_user.username,
                    display_name=mock_user.display_name,
                    email=mock_user.email,
                    groups=mock_user.groups,
                    auth_method="mock",
                    is_admin=(role == Role.ADMIN),
                    system_role=role,
                )
    return None


def _ldap_authenticate(username: str, password: str):
    user = ldap_service.authenticate(username, password)
    if user:
        role = _resolve_global_role(user.username, user.groups)
        user.system_role = role
        user.is_admin = role == Role.ADMIN
    return user


import sys
_this_module = sys.modules[__name__]

router = create_auth_router(
    settings_provider=lambda: getattr(_this_module, "settings", settings),
    storage_service=storage_service,
    get_current_user_dep=get_current_user,
    authenticate_mock_fn=lambda u, p: getattr(_this_module, "_mock_authenticate", _mock_authenticate)(u, p),
    authenticate_ldap_fn=lambda u, p: getattr(_this_module, "_ldap_authenticate", _ldap_authenticate)(u, p),
    get_ldap_user_info_fn=lambda u: getattr(_this_module, "ldap_service", ldap_service).get_user_info(u),
    acl_checker_fn=lambda u: getattr(_this_module, "check_ui_access", check_ui_access)(u),
    rate_limiter=auth_rate_limiter,
    kerberos_authenticator=lambda header: getattr(_this_module, "kerberos_manager", kerberos_manager).authenticate_spnego(header),
    cookie_name="yarn_explorer_session",
    additional_cookie_names=["hadoop_explorer_session"],
    prefix="/api/v1/auth",
)

__all__ = [
    "router",
    "kerberos_manager",
    "ldap_service",
    "storage_service",
    "_mock_authenticate",
    "_resolve_global_role",
]
