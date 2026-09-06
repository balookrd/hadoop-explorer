import logging
from typing import Optional, List
from backend.common.core.ldap_auth import CommonLdapAuthService
from app.core.config import settings
from app.models.auth import UserInfo
from app.core.acl import is_global_admin

logger = logging.getLogger(__name__)


class LdapClient:
    def __init__(self):
        self.config = settings.ldap

    def authenticate_ldap(self, username: str, password: str) -> Optional[UserInfo]:
        if not password:
            return None

        result = CommonLdapAuthService.authenticate_ldap_user(
            username=username,
            password=password,
            server_uri=self.config.server_uri,
            use_ssl=self.config.use_ssl,
            verify_cert=self.config.verify_cert,
            ca_cert_file=self.config.ca_cert_file,
            allow_insecure_ssl=getattr(self.config, "allow_insecure_ssl", False),
            bind_dn=self.config.bind_dn,
            bind_password=self.config.bind_password,
            user_base_dn=self.config.user_search_base,
            user_filter=self.config.user_search_filter,
            username_attr=self.config.username_attribute,
            display_name_attr=self.config.display_name_attribute,
            email_attr=self.config.email_attribute,
            memberof_attr=self.config.memberof_attribute,
            use_user_memberof=self.config.use_user_memberof,
            group_base_dn=self.config.group_search_base,
            group_filter=self.config.group_search_filter,
            group_name_attr=self.config.group_attribute,
            connect_timeout=10
        )
        if not result:
            return None

        groups = result["groups"]
        is_admin = is_global_admin(result["username"], groups)
        return UserInfo(
            username=result["username"],
            display_name=result["display_name"],
            email=result["email"],
            groups=groups,
            is_admin=is_admin
        )

    def get_user_info(self, username: str) -> Optional[UserInfo]:
        mode = getattr(settings, "auth", None) and settings.auth.mode or ("mock" if not self.config.enabled else "ldaps_only")
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
                            is_admin=is_admin
                        )
            return None

        result = CommonLdapAuthService.get_ldap_user_info(
            username=username,
            server_uri=self.config.server_uri,
            use_ssl=self.config.use_ssl,
            verify_cert=self.config.verify_cert,
            ca_cert_file=self.config.ca_cert_file,
            allow_insecure_ssl=getattr(self.config, "allow_insecure_ssl", False),
            bind_dn=self.config.bind_dn,
            bind_password=self.config.bind_password,
            user_base_dn=self.config.user_search_base,
            user_filter=self.config.user_search_filter,
            username_attr=self.config.username_attribute,
            display_name_attr=self.config.display_name_attribute,
            email_attr=self.config.email_attribute,
            memberof_attr=self.config.memberof_attribute,
            use_user_memberof=self.config.use_user_memberof,
            group_base_dn=self.config.group_search_base,
            group_filter=self.config.group_search_filter,
            group_name_attr=self.config.group_attribute,
            connect_timeout=10
        )
        if not result:
            return None

        groups = result["groups"]
        is_admin = is_global_admin(result["username"], groups)
        return UserInfo(
            username=result["username"],
            display_name=result["display_name"],
            email=result["email"],
            groups=groups,
            is_admin=is_admin
        )

    def authenticate_mock(self, username: str, password: str) -> Optional[UserInfo]:
        result = CommonLdapAuthService.authenticate_mock_user(
            username=username,
            password=password,
            mock_users=settings.mock_users
        )
        if not result:
            return None

        groups = result["groups"]
        is_admin = is_global_admin(result["username"], groups)
        return UserInfo(
            username=result["username"],
            display_name=result["display_name"],
            email=result["email"],
            groups=groups,
            is_admin=is_admin
        )

    def authenticate(self, username: str, password: str) -> Optional[UserInfo]:
        mode = getattr(settings, "auth", None) and settings.auth.mode or ("mock" if not self.config.enabled else "ldaps_only")
        if mode == "mock":
            return self.authenticate_mock(username, password)
        elif mode in ("hybrid", "ldaps_only") and self.config.enabled:
            return self.authenticate_ldap(username, password)
        return None


ldap_client = LdapClient()
