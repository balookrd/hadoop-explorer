import logging
from typing import Optional, Dict, Any
from backend.common.core.ldap_auth import LdapAuthService, CommonLdapAuthService
from app.core.config import settings

logger = logging.getLogger("ldap_auth")
ldap_service = LdapAuthService(settings.auth.ldap)


def authenticate_ldap(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Выполняет аутентификацию пользователя в Active Directory / OpenLDAP через LDAPS.
    """
    return ldap_service.authenticate(username, password)


def get_ldap_user_info(username: str) -> Optional[Dict[str, Any]]:
    """
    Выполняет поиск пользователя в Active Directory / OpenLDAP и извлекает его группы без проверки пароля.
    Используется для обогащения групп при Kerberos SPNEGO SSO.
    """
    return ldap_service.get_user_info(username)
