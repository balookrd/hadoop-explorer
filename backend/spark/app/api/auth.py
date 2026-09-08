from app.core.config import settings
from app.core.security import get_current_user, UserSession
from app.core.ldap_auth import authenticate_ldap, get_ldap_user_info
from app.services.storage import storage_service
from backend.common.core.rate_limiter import auth_rate_limiter
from backend.common.core.kerberos import kerberos_manager
from backend.common.models.auth import LoginRequest, TokenResponse as AuthResponse
from backend.common.api.auth_router import create_auth_router


def _check_spark_ui_access(user: UserSession) -> bool:
    admin_groups = set(settings.acl.ui_access.admin_groups)
    is_admin = bool(set(user.groups) & admin_groups)
    user.is_admin = is_admin
    return True


async def _on_spark_logout(username: str):
    from app.services.session_manager import session_manager
    await session_manager.stop_all_user_sessions(username)


router = create_auth_router(
    settings_provider=lambda: settings,
    storage_service=storage_service,
    get_current_user_dep=get_current_user,
    authenticate_ldap_fn=authenticate_ldap,
    get_ldap_user_info_fn=get_ldap_user_info,
    acl_checker_fn=_check_spark_ui_access,
    kerberos_authenticator=lambda header: kerberos_manager.authenticate_spnego(header),
    on_logout_fn=_on_spark_logout,
    cookie_name="spark_explorer_session",
    additional_cookie_names=["hadoop_explorer_session"],
    prefix="/auth",
)

__all__ = ["router", "storage_service", "kerberos_manager", "LoginRequest", "AuthResponse"]
