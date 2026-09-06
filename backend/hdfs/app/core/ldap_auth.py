import hmac
import ssl
import logging
from typing import Optional, List, Tuple
import ldap3
from ldap3.core.exceptions import LDAPException
from ldap3.utils.conv import escape_filter_chars

from app.core.config import settings
from app.models.auth import UserInfo
from app.core.acl import is_global_admin

logger = logging.getLogger(__name__)


class LdapClient:
    def __init__(self):
        self.config = settings.ldap

    def _get_tls_config(self) -> Optional[ldap3.Tls]:
        if not self.config.use_ssl:
            return None

        validate = ssl.CERT_REQUIRED if self.config.verify_cert else ssl.CERT_NONE
        ca_certs_file = self.config.ca_cert_file if self.config.ca_cert_file else None

        return ldap3.Tls(
            validate=validate,
            ca_certs_file=ca_certs_file
        )

    def _get_server(self) -> ldap3.Server:
        tls = self._get_tls_config()
        return ldap3.Server(
            self.config.server_uri,
            use_ssl=self.config.use_ssl,
            tls=tls,
            get_info=ldap3.ALL,
            connect_timeout=10
        )

    def authenticate_ldap(self, username: str, password: str) -> Optional[UserInfo]:
        if not password:
            return None

        server = self._get_server()

        try:
            # 1. Сервисный bind
            service_conn = ldap3.Connection(
                server,
                user=self.config.bind_dn,
                password=self.config.bind_password,
                auto_bind=True,
                read_only=True
            )
        except LDAPException as e:
            logger.error(f"Не удалось подключиться к LDAP под сервисным аккаунтом: {e}")
            return None

        try:
            # 2. Поиск DN пользователя с безопасным экранированием (защита от LDAP Injection / CWE-90)
            safe_username = escape_filter_chars(username)
            search_filter = self.config.user_search_filter.format(username=safe_username)
            attributes = [
                self.config.username_attribute,
                self.config.email_attribute,
                self.config.display_name_attribute,
            ]
            has_memberof_schema = bool(
                service_conn.server.schema
                and any(k.lower() == self.config.memberof_attribute.lower() for k in service_conn.server.schema.attribute_types.keys())
            )
            if has_memberof_schema or self.config.use_user_memberof:
                attributes.append(self.config.memberof_attribute)

            service_conn.search(
                search_base=self.config.user_search_base,
                search_filter=search_filter,
                search_scope=ldap3.SUBTREE,
                attributes=attributes
            )

            if not service_conn.entries:
                logger.warning(f"LDAP пользователь {username} не найден")
                return None

            user_entry = service_conn.entries[0]
            user_dn = user_entry.entry_dn

            # Извлечение данных пользователя
            display_name = getattr(user_entry, self.config.display_name_attribute).value or username
            email = getattr(user_entry, self.config.email_attribute).value if hasattr(user_entry, self.config.email_attribute) else None

            # 3. Проверка пароля путем бинда от имени пользователя
            user_conn = ldap3.Connection(
                server,
                user=user_dn,
                password=password,
                auto_bind=False
            )
            if not user_conn.bind():
                logger.warning(f"Неверный пароль LDAP для пользователя {username}")
                return None

            user_conn.unbind()

            # 4. Извлечение групп (гибридный поиск: memberOf + group_search_base)
            groups: List[str] = []
            if hasattr(user_entry, self.config.memberof_attribute):
                raw_memberof = getattr(user_entry, self.config.memberof_attribute).values
                for g in raw_memberof:
                    cn = [part[3:].strip() for part in str(g).split(",") if part.upper().startswith("CN=")]
                    if cn:
                        groups.append(cn[0])
                    else:
                        groups.append(str(g))

            if self.config.group_search_base and self.config.group_search_filter:
                safe_user_dn = escape_filter_chars(user_dn)
                group_filter = self.config.group_search_filter.format(user_dn=safe_user_dn, username=safe_username)
                service_conn.search(
                    search_base=self.config.group_search_base,
                    search_filter=group_filter,
                    search_scope=ldap3.SUBTREE,
                    attributes=[self.config.group_attribute]
                )
                for g_entry in service_conn.entries:
                    grp_val = getattr(g_entry, self.config.group_attribute).value
                    if grp_val:
                        groups.append(str(grp_val))

            service_conn.unbind()

            is_admin = is_global_admin(username, list(set(groups)))
            return UserInfo(
                username=username,
                display_name=display_name,
                email=email,
                groups=list(set(groups)),
                is_admin=is_admin
            )

        except LDAPException as e:
            logger.error(f"Ошибка при LDAP аутентификации пользователя {username}: {e}")
            return None
        finally:
            if service_conn.bound:
                service_conn.unbind()

    def get_user_info(self, username: str) -> Optional[UserInfo]:
        """
        Безопасно извлекает данные и группы пользователя без требования пароля.
        Используется для SPNEGO Kerberos SSO.
        """
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

        server = self._get_server()
        try:
            service_conn = ldap3.Connection(
                server,
                user=self.config.bind_dn,
                password=self.config.bind_password,
                auto_bind=True,
                read_only=True
            )
        except LDAPException as e:
            logger.error(f"Не удалось подключиться к LDAP под сервисным аккаунтом: {e}")
            return None

        try:
            safe_username = escape_filter_chars(username)
            search_filter = self.config.user_search_filter.format(username=safe_username)
            attributes = [
                self.config.username_attribute,
                self.config.email_attribute,
                self.config.display_name_attribute,
            ]
            has_memberof_schema = bool(
                service_conn.server.schema
                and any(k.lower() == self.config.memberof_attribute.lower() for k in service_conn.server.schema.attribute_types.keys())
            )
            if has_memberof_schema or self.config.use_user_memberof:
                attributes.append(self.config.memberof_attribute)

            service_conn.search(
                search_base=self.config.user_search_base,
                search_filter=search_filter,
                search_scope=ldap3.SUBTREE,
                attributes=attributes
            )

            if not service_conn.entries:
                return None

            user_entry = service_conn.entries[0]
            user_dn = user_entry.entry_dn

            display_name = getattr(user_entry, self.config.display_name_attribute).value or username
            email = getattr(user_entry, self.config.email_attribute).value if hasattr(user_entry, self.config.email_attribute) else None

            groups: List[str] = []
            if hasattr(user_entry, self.config.memberof_attribute):
                raw_memberof = getattr(user_entry, self.config.memberof_attribute).values
                for g in raw_memberof:
                    cn = [part[3:].strip() for part in str(g).split(",") if part.upper().startswith("CN=")]
                    if cn:
                        groups.append(cn[0])
                    else:
                        groups.append(str(g))

            if self.config.group_search_base and self.config.group_search_filter:
                safe_user_dn = escape_filter_chars(user_dn)
                group_filter = self.config.group_search_filter.format(user_dn=safe_user_dn, username=safe_username)
                service_conn.search(
                    search_base=self.config.group_search_base,
                    search_filter=group_filter,
                    search_scope=ldap3.SUBTREE,
                    attributes=[self.config.group_attribute]
                )
                for g_entry in service_conn.entries:
                    grp_val = getattr(g_entry, self.config.group_attribute).value
                    if grp_val:
                        groups.append(str(grp_val))

            is_admin = is_global_admin(username, list(set(groups)))
            return UserInfo(
                username=username,
                display_name=display_name,
                email=email,
                groups=list(set(groups)),
                is_admin=is_admin
            )
        except LDAPException as e:
            logger.error(f"Ошибка при получении профиля LDAP для {username}: {e}")
            return None
        finally:
            if service_conn.bound:
                service_conn.unbind()

    def authenticate_mock(self, username: str, password: str) -> Optional[UserInfo]:
        import hashlib
        for u in settings.mock_users:
            if u.username.lower() == username.lower():
                is_valid = False
                if u.password.startswith("pbkdf2:"):
                    # Формат: pbkdf2:sha256:iterations$salt$hex_hash
                    try:
                        _, algo, rest = u.password.split(":", 2)
                        iterations_str, salt, target_hash = rest.split("$", 2)
                        derived = hashlib.pbkdf2_hmac(
                            algo,
                            password.encode("utf-8"),
                            salt.encode("utf-8"),
                            int(iterations_str)
                        ).hex()
                        is_valid = hmac.compare_digest(derived, target_hash)
                    except Exception:
                        is_valid = False
                else:
                    # Открытый пароль для локальной разработки с защитой от timing attack
                    is_valid = hmac.compare_digest(u.password, password)

                if is_valid:
                    is_admin = is_global_admin(u.username, u.groups)
                    return UserInfo(
                        username=u.username,
                        display_name=u.display_name,
                        email=u.email,
                        groups=u.groups,
                        is_admin=is_admin
                    )
        return None

    def authenticate(self, username: str, password: str) -> Optional[UserInfo]:
        mode = getattr(settings, "auth", None) and settings.auth.mode or ("mock" if not self.config.enabled else "ldaps_only")
        if mode == "mock":
            return self.authenticate_mock(username, password)
        elif mode in ("hybrid", "ldaps_only") and self.config.enabled:
            return self.authenticate_ldap(username, password)
        return None


ldap_client = LdapClient()
