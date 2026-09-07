import logging
from typing import Optional
from backend.common.core.ldap_auth import LdapAuthService, CommonLdapAuthService
from app.core.config import settings
from app.models.auth import UserSession, Role

logger = logging.getLogger(__name__)


class LdapService:
    def __init__(self):
        self.service = LdapAuthService(settings.auth.ldap)

    @property
    def config(self):
        return self.service.config

    def authenticate(self, username: str, password: str) -> Optional[UserSession]:
        result = self.service.authenticate(username, password)
        if not result:
            return None

        return UserSession(
            username=username,
            display_name=result["display_name"] or username,
            email=result["email"],
            groups=result["groups"],
            auth_method="ldap",
            is_admin=False,
            system_role=Role.READER,
        )

    def get_user_info(self, username: str) -> Optional[UserSession]:
        result = self.service.get_user_info(username)
        if not result:
            return None

        return UserSession(
            username=username,
            display_name=result["display_name"] or username,
            email=result["email"],
            groups=result["groups"],
            auth_method="ldap",
            is_admin=False,
            system_role=Role.READER,
        )


ldap_service = LdapService()
