import ssl
import logging
from typing import Optional, Dict, Any, List
from ldap3 import Server, Connection, ALL, Tls, SUBTREE
from ldap3.core.exceptions import LDAPException, LDAPBindError
from ldap3.utils.conv import escape_filter_chars
from app.core.config import settings

logger = logging.getLogger("ldap_auth")

def authenticate_ldap(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Выполняет аутентификацию пользователя в Active Directory / OpenLDAP через LDAPS.
    Возвращает dict с информацией о пользователе или None при ошибке.
    """
    cfg = settings.auth.ldap
    if not cfg.enabled:
        return None

    tls_config = None
    if cfg.use_ssl:
        if cfg.ca_cert_file:
            tls_config = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=cfg.ca_cert_file)
        elif cfg.allow_insecure_ssl:
            logger.warning("ВНИМАНИЕ: Проверка сертификата LDAPS отключена (allow_insecure_ssl=True)")
            tls_config = Tls(validate=ssl.CERT_NONE)
        else:
            tls_config = Tls(validate=ssl.CERT_REQUIRED)

    try:
        # 1. Подключение к серверу каталогов
        server = Server(cfg.server_uri, use_ssl=cfg.use_ssl, tls=tls_config, get_info=ALL, connect_timeout=5)
        
        # 2. Сервисный BIND (если настроен) или анонимный поиск
        bind_user = cfg.bind_dn if cfg.bind_dn else None
        bind_pwd = cfg.bind_password if cfg.bind_password else None

        with Connection(server, user=bind_user, password=bind_pwd, auto_bind=True) as conn:
            # 3. Поиск пользователя по логину с защитой от LDAP Injection
            escaped_username = escape_filter_chars(username)
            search_filter = cfg.user_filter.format(username=escaped_username)
            attributes = [cfg.user_display_name_attr, cfg.user_email_attr]
            if conn.server.schema and any(k.lower() == "memberof" for k in conn.server.schema.attribute_types.keys()):
                attributes.append("memberOf")
            
            conn.search(
                search_base=cfg.user_base_dn,
                search_filter=search_filter,
                search_scope=SUBTREE,
                attributes=attributes
            )

            if not conn.entries:
                logger.warning(f"Пользователь {username} не найден в LDAP")
                return None

            user_entry = conn.entries[0]
            user_dn = user_entry.entry_dn

            # 4. Проверка пароля: попытка BIND от имени найденного пользователя
            user_conn = Connection(server, user=user_dn, password=password)
            if not user_conn.bind():
                logger.warning(f"Неверный пароль для пользователя {username}")
                return None
            user_conn.unbind()

            # 5. Извлечение групп
            groups = []
            if hasattr(user_entry, "memberOf"):
                for group_dn in user_entry.memberOf:
                    # Извлечение CN группы (например, "CN=bi-analysts,OU=Groups,DC=..." -> "bi-analysts")
                    cn_part = [p for p in str(group_dn).split(",") if p.lower().startswith("cn=")]
                    if cn_part:
                        groups.append(cn_part[0][3:].strip())
                    else:
                        groups.append(str(group_dn))

            # Если группы хранятся в group_base_dn с фильтром member={user_dn}
            if cfg.group_base_dn and not groups:
                group_filter = cfg.group_filter.format(
                    user_dn=escape_filter_chars(str(user_dn)),
                    username=escaped_username
                )
                conn.search(
                    search_base=cfg.group_base_dn,
                    search_filter=group_filter,
                    search_scope=SUBTREE,
                    attributes=[cfg.group_name_attr]
                )
                for entry in conn.entries:
                    if hasattr(entry, cfg.group_name_attr):
                        groups.append(str(getattr(entry, cfg.group_name_attr)))

            display_name = getattr(user_entry, cfg.user_display_name_attr, username)
            email = getattr(user_entry, cfg.user_email_attr, None)

            return {
                "username": username,
                "display_name": str(display_name) if display_name else username,
                "email": str(email) if email else None,
                "groups": list(set(groups)),
                "auth_method": "ldaps"
            }

    except (LDAPException, LDAPBindError) as e:
        logger.error(f"Ошибка при работе с LDAP: {e}", exc_info=True)
        return None


def get_ldap_user_info(username: str) -> Optional[Dict[str, Any]]:
    """
    Выполняет поиск пользователя в Active Directory / OpenLDAP и извлекает его группы без проверки пароля.
    Используется для обогащения групп при Kerberos SPNEGO SSO.
    """
    cfg = settings.auth.ldap
    if not cfg.enabled:
        return None

    tls_config = None
    if cfg.use_ssl:
        if cfg.ca_cert_file:
            tls_config = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=cfg.ca_cert_file)
        elif cfg.allow_insecure_ssl:
            logger.warning("ВНИМАНИЕ: Проверка сертификата LDAPS отключена (allow_insecure_ssl=True)")
            tls_config = Tls(validate=ssl.CERT_NONE)
        else:
            tls_config = Tls(validate=ssl.CERT_REQUIRED)

    try:
        server = Server(cfg.server_uri, use_ssl=cfg.use_ssl, tls=tls_config, get_info=ALL, connect_timeout=5)
        bind_user = cfg.bind_dn if cfg.bind_dn else None
        bind_pwd = cfg.bind_password if cfg.bind_password else None

        with Connection(server, user=bind_user, password=bind_pwd, auto_bind=True) as conn:
            escaped_username = escape_filter_chars(username)
            search_filter = cfg.user_filter.format(username=escaped_username)
            attributes = [cfg.user_display_name_attr, cfg.user_email_attr]
            if conn.server.schema and any(k.lower() == "memberof" for k in conn.server.schema.attribute_types.keys()):
                attributes.append("memberOf")
            
            conn.search(
                search_base=cfg.user_base_dn,
                search_filter=search_filter,
                search_scope=SUBTREE,
                attributes=attributes
            )

            if not conn.entries:
                logger.warning(f"Пользователь {username} не найден в LDAP")
                return None

            user_entry = conn.entries[0]
            user_dn = user_entry.entry_dn

            groups = []
            if hasattr(user_entry, "memberOf"):
                for group_dn in user_entry.memberOf:
                    cn_part = [p for p in str(group_dn).split(",") if p.lower().startswith("cn=")]
                    if cn_part:
                        groups.append(cn_part[0][3:].strip())
                    else:
                        groups.append(str(group_dn))

            if cfg.group_base_dn and not groups:
                group_filter = cfg.group_filter.format(
                    user_dn=escape_filter_chars(str(user_dn)),
                    username=escaped_username
                )
                conn.search(
                    search_base=cfg.group_base_dn,
                    search_filter=group_filter,
                    search_scope=SUBTREE,
                    attributes=[cfg.group_name_attr]
                )
                for entry in conn.entries:
                    if hasattr(entry, cfg.group_name_attr):
                        groups.append(str(getattr(entry, cfg.group_name_attr)))

            display_name = getattr(user_entry, cfg.user_display_name_attr, username)
            email = getattr(user_entry, cfg.user_email_attr, None)

            return {
                "username": username,
                "display_name": str(display_name) if display_name else username,
                "email": str(email) if email else None,
                "groups": list(set(groups)),
                "auth_method": "ldaps"
            }

    except (LDAPException, LDAPBindError) as e:
        logger.error(f"Ошибка при поиске пользователя в LDAP: {e}", exc_info=True)
        return None
