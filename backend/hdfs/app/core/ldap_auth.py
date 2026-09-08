import logging
from typing import Optional
from backend.common.core.ldap_auth import LdapAuthService, CommonLdapAuthService
from app.core.config import settings
from backend.common.models.auth import UserInfo
from app.core.acl import is_global_admin

logger = logging.getLogger(__name__)


class LdapClient:
    def __init__(self):
        self.service = LdapAuthService(settings.ldap)

    @property
    def config(self):
        return self.service.config

    def authenticate_ldap(self, username: str, password: str) -> Optional[UserInfo]:
        result = self.service.authenticate(username, password)
        if not result:
            return None

        groups = result["groups"]
        is_admin = is_global_admin(result["username"], groups)
        return UserInfo(
            username=result["username"],
            display_name=result["display_name"],
            email=result["email"],
            groups=groups,
            is_admin=is_admin,
        )

    def get_user_info(self, username: str) -> Optional[UserInfo]:
        mode = (
            getattr(settings, "auth", None)
            and settings.auth.mode
            or ("mock" if not self.config.enabled else "ldaps_only")
        )
        if not self.config.enabled:
            if mode == "mock":
                for u in settings.mock_users:
                    if u.username.lower() == username.lower():
                        is_admin = is_global_admin(u.username, u.groups)
                        return UserInfo(
                            username=u.username,
                            display_name=u.display_name,
                            email=u.email,
                            groups=u.groups,
                            is_admin=is_admin,
                        )
            return None

        result = self.service.get_user_info(username)
        if not result:
            return None

        groups = result["groups"]
        is_admin = is_global_admin(result["username"], groups)
        return UserInfo(
            username=result["username"],
            display_name=result["display_name"],
            email=result["email"],
            groups=groups,
            is_admin=is_admin,
        )

    def authenticate_mock(self, username: str, password: str) -> Optional[UserInfo]:
        result = self.service.authenticate_mock(username=username, password=password, mock_users=settings.mock_users)
        if not result:
            return None

        groups = result["groups"]
        is_admin = is_global_admin(result["username"], groups)
        return UserInfo(
            username=result["username"],
            display_name=result["display_name"],
            email=result["email"],
            groups=groups,
            is_admin=is_admin,
        )

    def authenticate(self, username: str, password: str) -> Optional[UserInfo]:
        mode = (
            getattr(settings, "auth", None)
            and settings.auth.mode
            or ("mock" if not self.config.enabled else "ldaps_only")
        )
        if mode == "mock":
            return self.authenticate_mock(username, password)
        elif mode in ("hybrid", "ldaps_only") and self.config.enabled:
            return self.authenticate_ldap(username, password)
        return None


ldap_client = LdapClient()
