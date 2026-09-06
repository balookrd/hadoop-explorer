import logging
from typing import Optional, Dict, Any
from backend.common.core.ldap_auth import CommonLdapAuthService
from app.core.config import settings

logger = logging.getLogger("ldap_auth")


def authenticate_ldap(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Выполняет аутентификацию пользователя в Active Directory / OpenLDAP через LDAPS.
    Использует CommonLdapAuthService из backend.common.
    """
    cfg = settings.auth.ldap
    if not cfg.enabled:
        return None

    return CommonLdapAuthService.authenticate_ldap_user(
        username=username,
        password=password,
        server_uri=cfg.server_uri,
        use_ssl=cfg.use_ssl,
        verify_cert=getattr(cfg, "verify_cert", True),
        ca_cert_file=cfg.ca_cert_file,
        allow_insecure_ssl=getattr(cfg, "allow_insecure_ssl", False),
        bind_dn=cfg.bind_dn,
        bind_password=cfg.bind_password,
        user_base_dn=cfg.user_base_dn,
        user_filter=cfg.user_filter,
        username_attr="sAMAccountName",
        display_name_attr=cfg.user_display_name_attr,
        email_attr=cfg.user_email_attr,
        memberof_attr="memberOf",
        use_user_memberof=True,
        group_base_dn=cfg.group_base_dn,
        group_filter=cfg.group_filter,
        group_name_attr=cfg.group_name_attr,
        connect_timeout=5
    )


def get_ldap_user_info(username: str) -> Optional[Dict[str, Any]]:
    """
    Выполняет поиск пользователя в Active Directory / OpenLDAP и извлекает его группы без проверки пароля.
    Используется для обогащения групп при Kerberos SPNEGO SSO.
    """
    cfg = settings.auth.ldap
    if not cfg.enabled:
        return None

    return CommonLdapAuthService.get_ldap_user_info(
        username=username,
        server_uri=cfg.server_uri,
        use_ssl=cfg.use_ssl,
        verify_cert=getattr(cfg, "verify_cert", True),
        ca_cert_file=cfg.ca_cert_file,
        allow_insecure_ssl=getattr(cfg, "allow_insecure_ssl", False),
        bind_dn=cfg.bind_dn,
        bind_password=cfg.bind_password,
        user_base_dn=cfg.user_base_dn,
        user_filter=cfg.user_filter,
        username_attr="sAMAccountName",
        display_name_attr=cfg.user_display_name_attr,
        email_attr=cfg.user_email_attr,
        memberof_attr="memberOf",
        use_user_memberof=True,
        group_base_dn=cfg.group_base_dn,
        group_filter=cfg.group_filter,
        group_name_attr=cfg.group_name_attr,
        connect_timeout=5
    )
