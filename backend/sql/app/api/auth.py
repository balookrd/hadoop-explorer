import sys
from app.core.config import settings
from app.core.security import get_current_user, UserSession
from app.core.acl import check_ui_access
from app.core.ldap_auth import authenticate_ldap, get_ldap_user_info
from app.core.rate_limiter import auth_rate_limiter
from app.services.storage import storage_service
from backend.common.core.kerberos import kerberos_manager, authenticate_spnego
from backend.common.models.auth import LoginRequest, TokenResponse as AuthResponse
from backend.common.api.auth_router import create_auth_router

_this_module = sys.modules[__name__]

router = create_auth_router(
    settings_provider=lambda: settings,
    storage_service=storage_service,
    get_current_user_dep=get_current_user,
    authenticate_ldap_fn=lambda u, p: getattr(_this_module, "authenticate_ldap", authenticate_ldap)(u, p),
    get_ldap_user_info_fn=lambda u: getattr(_this_module, "get_ldap_user_info", get_ldap_user_info)(u),
    acl_checker_fn=lambda u: getattr(_this_module, "check_ui_access", check_ui_access)(u),
    kerberos_authenticator=lambda h: getattr(_this_module, "authenticate_spnego", authenticate_spnego)(h),
    rate_limiter=auth_rate_limiter,
    cookie_name="access_token",
    additional_cookie_names=["session_token", "sql_explorer_session"],
    prefix="/auth",
)

__all__ = [
    "router",
    "storage_service",
    "kerberos_manager",
    "authenticate_spnego",
    "get_ldap_user_info",
    "authenticate_ldap",
    "check_ui_access",
    "LoginRequest",
    "AuthResponse",
]
