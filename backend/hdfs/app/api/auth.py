from app.core.config import settings
from app.core.ldap_auth import ldap_client
from app.services.storage import storage_service
from app.core.security import get_current_user
from app.core.acl import can_access_ui
from app.core.rate_limiter import auth_rate_limiter
from backend.common.core.kerberos import kerberos_manager
from backend.common.api.auth_router import create_auth_router

router = create_auth_router(
    settings_provider=lambda: settings,
    storage_service=storage_service,
    get_current_user_dep=get_current_user,
    authenticate_mock_fn=ldap_client.authenticate_mock,
    authenticate_ldap_fn=ldap_client.authenticate_ldap,
    get_ldap_user_info_fn=ldap_client.get_user_info,
    acl_checker_fn=lambda u: can_access_ui(u.username, u.groups),
    rate_limiter=auth_rate_limiter,
    kerberos_authenticator=lambda header: kerberos_manager.authenticate_spnego(header),
    cookie_name="access_token",
    additional_cookie_names=["hdfs_explorer_session", "session_token"],
    prefix="/api/v1/auth",
)

__all__ = ["router", "kerberos_manager", "ldap_client", "storage_service"]
