import logging
from typing import Optional, List, Tuple
from ldap3 import Server, Connection, ALL, Tls, SUBTREE
from ldap3.utils.conv import escape_filter_chars
import ssl

from app.core.config import settings
from app.models.auth import UserSession, Role

logger = logging.getLogger(__name__)


class LdapService:
    def __init__(self):
        self.config = settings.auth.ldap

    def authenticate(self, username: str, password: str) -> Optional[UserSession]:
        if not self.config.enabled:
            return None

        if not password:
            return None

        try:
            tls_configuration = None
            if self.config.use_ssl:
                validate_mode = ssl.CERT_REQUIRED
                if not self.config.verify_cert or self.config.allow_insecure_ssl:
                    logger.warning("ВНИМАНИЕ: Проверка сертификата LDAPS отключена")
                    validate_mode = ssl.CERT_NONE
                tls_configuration = Tls(
                    validate=validate_mode,
                    ca_certs_file=self.config.ca_cert_file if self.config.ca_cert_file else None
                )

            server = Server(
                self.config.server_uri,
                use_ssl=self.config.use_ssl,
                tls=tls_configuration,
                get_info=ALL,
                connect_timeout=5
            )

            # 1. Сервисный bind
            bind_dn = self.config.bind_dn or None
            bind_password = self.config.bind_password or None

            with Connection(server, user=bind_dn, password=bind_password, auto_bind=True) as conn:
                # 2. Поиск пользователя (экранируем ввод для защиты от LDAP-инъекций)
                safe_username = escape_filter_chars(username)
                search_filter = self.config.user_filter.format(username=safe_username)
                attributes = [
                    self.config.user_display_name_attr,
                    self.config.user_email_attr
                ]
                if conn.server.schema and any(k.lower() == "memberof" for k in conn.server.schema.attribute_types.keys()):
                    attributes.append("memberOf")

                conn.search(
                    search_base=self.config.user_base_dn,
                    search_filter=search_filter,
                    search_scope=SUBTREE,
                    attributes=attributes
                )

                if not conn.entries:
                    logger.warning(f"LDAP: Пользователь {username} не найден")
                    return None

                user_entry = conn.entries[0]
                user_dn = user_entry.entry_dn

                # 3. Аутентификация пользователя (User bind)
                with Connection(server, user=user_dn, password=password) as user_conn:
                    if not user_conn.bind():
                        logger.warning(f"LDAP: Неверный пароль для пользователя {username}")
                        return None

                # 4. Извлечение атрибутов и групп
                display_name = (
                    str(getattr(user_entry, self.config.user_display_name_attr, username))
                    if hasattr(user_entry, self.config.user_display_name_attr)
                    else username
                )
                email = (
                    str(getattr(user_entry, self.config.user_email_attr, ""))
                    if hasattr(user_entry, self.config.user_email_attr)
                    else None
                )

                # Извлечение групп
                groups = self._extract_groups(conn, user_dn, user_entry)

                return UserSession(
                    username=username,
                    display_name=display_name or username,
                    email=email,
                    groups=groups,
                    auth_method="ldap",
                    is_admin=False,
                    system_role=Role.READER
                )

        except Exception as e:
            logger.error(f"LDAP ошибка аутентификации: {e}")
            return None

    def _extract_groups(self, conn: Connection, user_dn: str, user_entry) -> List[str]:
        groups = []
        try:
            # Способ 1: memberOf атрибут
            if hasattr(user_entry, "memberOf"):
                for m in user_entry.memberOf:
                    val = str(m)
                    cn_part = [part[3:] for part in val.split(",") if part.upper().startswith("CN=")]
                    if cn_part:
                        groups.append(cn_part[0])
                    else:
                        groups.append(val)

            # Способ 2: явный поиск в group_base_dn
            if self.config.group_base_dn and self.config.group_filter:
                safe_dn = escape_filter_chars(str(user_dn))
                g_filter = self.config.group_filter.format(user_dn=safe_dn)
                conn.search(
                    search_base=self.config.group_base_dn,
                    search_filter=g_filter,
                    search_scope=SUBTREE,
                    attributes=[self.config.group_name_attr]
                )
                for entry in conn.entries:
                    gname = getattr(entry, self.config.group_name_attr, None)
                    if gname:
                        groups.append(str(gname))
        except Exception as e:
            logger.warning(f"LDAP ошибка получения групп: {e}")

        return list(set(groups))

    def get_user_info(self, username: str) -> Optional[UserSession]:
        """
        Извлекает информацию о пользователе и его группы без необходимости проверки пароля.
        Используется для обогащения групп при Kerberos SPNEGO SSO.
        """
        if not self.config.enabled:
            return None

        try:
            tls_configuration = None
            if self.config.use_ssl:
                validate_mode = ssl.CERT_REQUIRED
                if not self.config.verify_cert or self.config.allow_insecure_ssl:
                    logger.warning("ВНИМАНИЕ: Проверка сертификата LDAPS отключена")
                    validate_mode = ssl.CERT_NONE
                tls_configuration = Tls(
                    validate=validate_mode,
                    ca_certs_file=self.config.ca_cert_file if self.config.ca_cert_file else None
                )

            server = Server(
                self.config.server_uri,
                use_ssl=self.config.use_ssl,
                tls=tls_configuration,
                get_info=ALL,
                connect_timeout=5
            )

            bind_dn = self.config.bind_dn or None
            bind_password = self.config.bind_password or None

            with Connection(server, user=bind_dn, password=bind_password, auto_bind=True) as conn:
                safe_username = escape_filter_chars(username)
                search_filter = self.config.user_filter.format(username=safe_username)
                attributes = [
                    self.config.user_display_name_attr,
                    self.config.user_email_attr
                ]
                if conn.server.schema and any(k.lower() == "memberof" for k in conn.server.schema.attribute_types.keys()):
                    attributes.append("memberOf")

                conn.search(
                    search_base=self.config.user_base_dn,
                    search_filter=search_filter,
                    search_scope=SUBTREE,
                    attributes=attributes
                )

                if not conn.entries:
                    logger.warning(f"LDAP: Пользователь {username} не найден")
                    return None

                user_entry = conn.entries[0]
                user_dn = user_entry.entry_dn

                display_name = (
                    str(getattr(user_entry, self.config.user_display_name_attr, username))
                    if hasattr(user_entry, self.config.user_display_name_attr)
                    else username
                )
                email = (
                    str(getattr(user_entry, self.config.user_email_attr, ""))
                    if hasattr(user_entry, self.config.user_email_attr)
                    else None
                )

                groups = self._extract_groups(conn, user_dn, user_entry)

                return UserSession(
                    username=username,
                    display_name=display_name or username,
                    email=email,
                    groups=groups,
                    auth_method="ldap",
                    is_admin=False,
                    system_role=Role.READER
                )
        except Exception as e:
            logger.error(f"LDAP ошибка получения информации о пользователе {username}: {e}")
            return None


ldap_service = LdapService()
