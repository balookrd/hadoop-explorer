import logging
from typing import Optional
from ldap3 import Connection, Server, Tls, ALL, SUBTREE
from ldap3.utils.conv import escape_filter_chars
from backend.common.core.ldap_auth import CommonLdapAuthService
from app.core.config import settings
from app.models.auth import UserSession, Role

logger = logging.getLogger(__name__)


class LdapService:
    def __init__(self):
        self.config = settings.auth.ldap

    def authenticate(self, username: str, password: str) -> Optional[UserSession]:
        if not self.config.enabled or not password:
            return None

        result = CommonLdapAuthService.authenticate_ldap_user(
            username=username,
            password=password,
            server_uri=self.config.server_uri,
            use_ssl=self.config.use_ssl,
            verify_cert=getattr(self.config, "verify_cert", True),
            ca_cert_file=self.config.ca_cert_file,
            allow_insecure_ssl=getattr(self.config, "allow_insecure_ssl", False),
            bind_dn=self.config.bind_dn,
            bind_password=self.config.bind_password,
            user_base_dn=self.config.user_base_dn,
            user_filter=self.config.user_filter,
            username_attr=getattr(self.config, "username_attr", None),
            display_name_attr=self.config.user_display_name_attr,
            email_attr=self.config.user_email_attr,
            memberof_attr=getattr(self.config, "memberof_attr", "memberOf"),
            use_user_memberof=getattr(self.config, "use_user_memberof", False),
            group_base_dn=self.config.group_base_dn,
            group_filter=self.config.group_filter,
            group_name_attr=self.config.group_name_attr,
            connect_timeout=5,
            connection_cls=Connection,
            server_cls=Server
        )
        if not result:
            return None

        return UserSession(
            username=username,
            display_name=result["display_name"] or username,
            email=result["email"],
            groups=result["groups"],
            auth_method="ldap",
            is_admin=False,
            system_role=Role.READER
        )

    def get_user_info(self, username: str) -> Optional[UserSession]:
        if not self.config.enabled or not username:
            return None

        result = CommonLdapAuthService.get_ldap_user_info(
            username=username,
            server_uri=self.config.server_uri,
            use_ssl=self.config.use_ssl,
            verify_cert=getattr(self.config, "verify_cert", True),
            ca_cert_file=self.config.ca_cert_file,
            allow_insecure_ssl=getattr(self.config, "allow_insecure_ssl", False),
            bind_dn=self.config.bind_dn,
            bind_password=self.config.bind_password,
            user_base_dn=self.config.user_base_dn,
            user_filter=self.config.user_filter,
            username_attr=getattr(self.config, "username_attr", None),
            display_name_attr=self.config.user_display_name_attr,
            email_attr=self.config.user_email_attr,
            memberof_attr=getattr(self.config, "memberof_attr", "memberOf"),
            use_user_memberof=getattr(self.config, "use_user_memberof", False),
            group_base_dn=self.config.group_base_dn,
            group_filter=self.config.group_filter,
            group_name_attr=self.config.group_name_attr,
            connect_timeout=5,
            connection_cls=Connection,
            server_cls=Server
        )
        if not result:
            return None

        return UserSession(
            username=username,
            display_name=result["display_name"] or username,
            email=result["email"],
            groups=result["groups"],
            auth_method="ldap",
            is_admin=False,
            system_role=Role.READER
        )


ldap_service = LdapService()
