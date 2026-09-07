import ssl
import hmac
import hashlib
import logging
from typing import Optional, List, Dict, Any, Union

import ldap3
from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.core.exceptions import LDAPException, LDAPBindError
from ldap3.utils.conv import escape_filter_chars

logger = logging.getLogger("hadoop_explorer.ldap")


class CommonLdapAuthService:
    """
    Единый сервис аутентификации LDAP/LDAPS и Mock пользователей для всех сервисов платформы.
    Поддерживает:
    - TLS/SSL конфигурацию с поддержкой CA-сертификатов и проверкой валидности.
    - Сервисный bind (если настроен) и поиск пользователей с защитой от LDAP Injection (CWE-90).
    - Проверку паролей через User bind.
    - Гибридное извлечение групп (через атрибут memberOf и/или поиск по group_base_dn).
    - Извлечение профиля пользователя без пароля для SPNEGO/Kerberos SSO (get_user_info).
    - Безопасную аутентификацию mock-пользователей с PBKDF2 хэшированием и защитой от timing-атак.
    """

    @staticmethod
    def get_tls_config(
        use_ssl: bool = False,
        verify_cert: bool = True,
        ca_cert_file: Optional[str] = None,
        allow_insecure_ssl: bool = False
    ) -> Optional[Tls]:
        """
        Формирует объект ldap3.Tls в соответствии с настройками безопасности.
        """
        if not use_ssl:
            return None

        validate = ssl.CERT_REQUIRED
        if allow_insecure_ssl or not verify_cert:
            logger.warning("ВНИМАНИЕ: Проверка сертификата LDAPS отключена (allow_insecure_ssl/verify_cert=False)")
            validate = ssl.CERT_NONE

        return Tls(
            validate=validate,
            ca_certs_file=ca_cert_file if ca_cert_file else None
        )

    @classmethod
    def get_server(
        cls,
        server_uri: str,
        use_ssl: bool = False,
        verify_cert: bool = True,
        ca_cert_file: Optional[str] = None,
        allow_insecure_ssl: bool = False,
        connect_timeout: int = 5,
        server_cls: Any = Server
    ) -> Server:
        """
        Создает объект ldap3.Server с заданными параметрами TLS.
        """
        tls = cls.get_tls_config(
            use_ssl=use_ssl,
            verify_cert=verify_cert,
            ca_cert_file=ca_cert_file,
            allow_insecure_ssl=allow_insecure_ssl
        )
        return server_cls(
            server_uri,
            use_ssl=use_ssl,
            tls=tls,
            get_info=ALL,
            connect_timeout=connect_timeout
        )

    @classmethod
    def authenticate_ldap_user(
        cls,
        username: str,
        password: str,
        server_uri: str,
        use_ssl: bool = False,
        verify_cert: bool = True,
        ca_cert_file: Optional[str] = None,
        allow_insecure_ssl: bool = False,
        bind_dn: Optional[str] = None,
        bind_password: Optional[str] = None,
        user_base_dn: str = "dc=example,dc=com",
        user_filter: str = "(&(objectClass=user)(sAMAccountName={username}))",
        username_attr: Optional[str] = None,
        display_name_attr: Optional[str] = "displayName",
        email_attr: Optional[str] = "mail",
        memberof_attr: Optional[str] = "memberOf",
        use_user_memberof: bool = False,
        group_base_dn: Optional[str] = None,
        group_filter: Optional[str] = None,
        group_name_attr: str = "cn",
        connect_timeout: int = 5,
        connection_cls: Any = Connection,
        server_cls: Any = Server
    ) -> Optional[Dict[str, Any]]:
        """
        Выполняет аутентификацию пользователя в Active Directory / OpenLDAP через LDAP/LDAPS.
        Возвращает dict с данными пользователя или None при ошибке.
        """
        if not password or not username:
            return None

        server = cls.get_server(
            server_uri=server_uri,
            use_ssl=use_ssl,
            verify_cert=verify_cert,
            ca_cert_file=ca_cert_file,
            allow_insecure_ssl=allow_insecure_ssl,
            connect_timeout=connect_timeout,
            server_cls=server_cls
        )

        service_user = bind_dn if bind_dn else None
        service_pwd = bind_password if bind_password else None

        try:
            # 1. Сервисный bind для поиска DN пользователя
            with connection_cls(server, user=service_user, password=service_pwd, auto_bind=True, read_only=True) as service_conn:
                # 2. Поиск DN пользователя с безопасным экранированием (защита от CWE-90)
                safe_username = escape_filter_chars(username)
                search_filter = user_filter.format(username=safe_username)

                attributes = []
                if username_attr:
                    attributes.append(username_attr)
                if email_attr and email_attr not in attributes:
                    attributes.append(email_attr)
                if display_name_attr and display_name_attr not in attributes:
                    attributes.append(display_name_attr)

                # Проверяем memberOf
                if memberof_attr:
                    server_obj = getattr(service_conn, "server", None)
                    schema = getattr(server_obj, "schema", None) if server_obj else None
                    has_memberof_schema = bool(
                        schema
                        and hasattr(schema, "attribute_types")
                        and any(k.lower() == memberof_attr.lower() for k in schema.attribute_types.keys())
                    )
                    if use_user_memberof or has_memberof_schema:
                        if memberof_attr not in attributes:
                            attributes.append(memberof_attr)

                if not attributes:
                    attributes = ["*"]

                service_conn.search(
                    search_base=user_base_dn,
                    search_filter=search_filter,
                    search_scope=SUBTREE,
                    attributes=attributes
                )

                if not getattr(service_conn, "entries", None):
                    logger.warning(f"LDAP пользователь '{username}' не найден")
                    return None

                user_entry = service_conn.entries[0]
                user_dn = getattr(user_entry, "entry_dn", str(user_entry))

                # 3. Проверка пароля пользователя через User bind
                with connection_cls(server, user=user_dn, password=password, auto_bind=False) as user_conn:
                    if not user_conn.bind():
                        logger.warning(f"Неверный пароль LDAP для пользователя '{username}'")
                        return None

                # 4. Извлечение атрибутов профиля
                display_name = None
                if display_name_attr and hasattr(user_entry, display_name_attr):
                    attr_val = getattr(user_entry, display_name_attr)
                    display_name = attr_val.value if hasattr(attr_val, "value") else str(attr_val)
                if not display_name:
                    display_name = username

                email = None
                if email_attr and hasattr(user_entry, email_attr):
                    attr_val = getattr(user_entry, email_attr)
                    email = attr_val.value if hasattr(attr_val, "value") else str(attr_val)

                # 5. Извлечение групп
                groups = cls._extract_groups_from_entry(
                    service_conn=service_conn,
                    user_dn=str(user_dn),
                    user_entry=user_entry,
                    safe_username=safe_username,
                    memberof_attr=memberof_attr,
                    group_base_dn=group_base_dn,
                    group_filter=group_filter,
                    group_name_attr=group_name_attr
                )

                return {
                    "username": username,
                    "display_name": str(display_name) if display_name else username,
                    "email": str(email) if email else None,
                    "groups": list(set(groups)),
                    "auth_method": "ldaps" if use_ssl else "ldap"
                }

        except (LDAPException, LDAPBindError) as e:
            logger.error(f"Ошибка при LDAP аутентификации пользователя '{username}': {e}")
            return None
        except Exception as e:
            logger.error(f"Непредвиденная ошибка LDAP аутентификации: {e}", exc_info=True)
            return None

    @classmethod
    def get_ldap_user_info(
        cls,
        username: str,
        server_uri: str,
        use_ssl: bool = False,
        verify_cert: bool = True,
        ca_cert_file: Optional[str] = None,
        allow_insecure_ssl: bool = False,
        bind_dn: Optional[str] = None,
        bind_password: Optional[str] = None,
        user_base_dn: str = "dc=example,dc=com",
        user_filter: str = "(&(objectClass=user)(sAMAccountName={username}))",
        username_attr: Optional[str] = None,
        display_name_attr: Optional[str] = "displayName",
        email_attr: Optional[str] = "mail",
        memberof_attr: Optional[str] = "memberOf",
        use_user_memberof: bool = False,
        group_base_dn: Optional[str] = None,
        group_filter: Optional[str] = None,
        group_name_attr: str = "cn",
        connect_timeout: int = 5,
        connection_cls: Any = Connection,
        server_cls: Any = Server
    ) -> Optional[Dict[str, Any]]:
        """
        Извлекает информацию о пользователе и его группы без требования пароля.
        Используется для обогащения групп при Kerberos SPNEGO SSO.
        """
        if not username:
            return None

        server = cls.get_server(
            server_uri=server_uri,
            use_ssl=use_ssl,
            verify_cert=verify_cert,
            ca_cert_file=ca_cert_file,
            allow_insecure_ssl=allow_insecure_ssl,
            connect_timeout=connect_timeout,
            server_cls=server_cls
        )

        service_user = bind_dn if bind_dn else None
        service_pwd = bind_password if bind_password else None

        try:
            with connection_cls(server, user=service_user, password=service_pwd, auto_bind=True, read_only=True) as service_conn:
                safe_username = escape_filter_chars(username)
                search_filter = user_filter.format(username=safe_username)

                attributes = []
                if username_attr:
                    attributes.append(username_attr)
                if email_attr and email_attr not in attributes:
                    attributes.append(email_attr)
                if display_name_attr and display_name_attr not in attributes:
                    attributes.append(display_name_attr)

                if memberof_attr:
                    server_obj = getattr(service_conn, "server", None)
                    schema = getattr(server_obj, "schema", None) if server_obj else None
                    has_memberof_schema = bool(
                        schema
                        and hasattr(schema, "attribute_types")
                        and any(k.lower() == memberof_attr.lower() for k in schema.attribute_types.keys())
                    )
                    if use_user_memberof or has_memberof_schema:
                        if memberof_attr not in attributes:
                            attributes.append(memberof_attr)

                if not attributes:
                    attributes = ["*"]

                service_conn.search(
                    search_base=user_base_dn,
                    search_filter=search_filter,
                    search_scope=SUBTREE,
                    attributes=attributes
                )

                if not getattr(service_conn, "entries", None):
                    logger.warning(f"LDAP пользователь '{username}' не найден при поиске инфо")
                    return None

                user_entry = service_conn.entries[0]
                user_dn = getattr(user_entry, "entry_dn", str(user_entry))

                display_name = None
                if display_name_attr and hasattr(user_entry, display_name_attr):
                    attr_val = getattr(user_entry, display_name_attr)
                    display_name = attr_val.value if hasattr(attr_val, "value") else str(attr_val)
                if not display_name:
                    display_name = username

                email = (
                    getattr(user_entry, email_attr).value
                    if hasattr(user_entry, email_attr) and hasattr(getattr(user_entry, email_attr), "value")
                    else getattr(user_entry, email_attr, None)
                )

                groups = cls._extract_groups_from_entry(
                    service_conn=service_conn,
                    user_dn=str(user_dn),
                    user_entry=user_entry,
                    safe_username=safe_username,
                    memberof_attr=memberof_attr,
                    group_base_dn=group_base_dn,
                    group_filter=group_filter,
                    group_name_attr=group_name_attr
                )

                return {
                    "username": username,
                    "display_name": str(display_name) if display_name else username,
                    "email": str(email) if email else None,
                    "groups": list(set(groups)),
                    "auth_method": "ldaps" if use_ssl else "ldap"
                }

        except (LDAPException, LDAPBindError) as e:
            logger.error(f"Ошибка при получении профиля LDAP для '{username}': {e}")
            return None
        except Exception as e:
            logger.error(f"Непредвиденная ошибка получения профиля LDAP: {e}", exc_info=True)
            return None

    @classmethod
    def _extract_groups_from_entry(
        cls,
        service_conn: Any,
        user_dn: str,
        user_entry: Any,
        safe_username: str,
        memberof_attr: str = "memberOf",
        group_base_dn: Optional[str] = None,
        group_filter: Optional[str] = None,
        group_name_attr: str = "cn"
    ) -> List[str]:
        groups: List[str] = []

        # 1. Извлечение из атрибута memberOf
        if hasattr(user_entry, memberof_attr):
            raw_attr = getattr(user_entry, memberof_attr)
            raw_memberof = getattr(raw_attr, "values", None) or (raw_attr if isinstance(raw_attr, list) else [raw_attr])
            for g in raw_memberof:
                cn_part = [part[3:].strip() for part in str(g).split(",") if part.upper().startswith("CN=")]
                if cn_part:
                    groups.append(cn_part[0])
                else:
                    groups.append(str(g))

        # 2. Поиск в group_base_dn
        if group_base_dn and group_filter:
            try:
                safe_user_dn = escape_filter_chars(user_dn)
                try:
                    g_filter = group_filter.format(user_dn=safe_user_dn, username=safe_username)
                except KeyError:
                    g_filter = group_filter.format(user_dn=safe_user_dn)

                service_conn.search(
                    search_base=group_base_dn,
                    search_filter=g_filter,
                    search_scope=SUBTREE,
                    attributes=[group_name_attr]
                )
                entries = getattr(service_conn, "entries", [])
                for g_entry in entries:
                    grp_val = getattr(g_entry, group_name_attr, None)
                    if grp_val:
                        val = grp_val.value if hasattr(grp_val, "value") else str(grp_val)
                        if val:
                            groups.append(str(val))
            except Exception as e:
                logger.warning(f"Ошибка поиска групп по group_base_dn: {e}")

        return list(set(groups))

    @staticmethod
    def verify_mock_password(stored_password: str, input_password: str) -> bool:
        if not stored_password or not input_password:
            return False
        if stored_password.startswith("pbkdf2:"):
            try:
                _, algo, rest = stored_password.split(":", 2)
                iterations_str, salt, target_hash = rest.split("$", 2)
                derived = hashlib.pbkdf2_hmac(
                    algo,
                    input_password.encode("utf-8"),
                    salt.encode("utf-8"),
                    int(iterations_str)
                ).hex()
                return hmac.compare_digest(derived, target_hash)
            except Exception:
                return False
        return hmac.compare_digest(stored_password, input_password)

    @classmethod
    def authenticate_mock_user(
        cls,
        username: str,
        password: str,
        mock_users: List[Any]
    ) -> Optional[Dict[str, Any]]:
        for u in mock_users:
            u_name = getattr(u, "username", None) or (u.get("username") if isinstance(u, dict) else None)
            if u_name and u_name.lower() == username.lower():
                u_pass = getattr(u, "password", None) or (u.get("password") if isinstance(u, dict) else "")
                if u_pass and cls.verify_mock_password(u_pass, password):
                    display_name = getattr(u, "display_name", None) or (u.get("display_name") if isinstance(u, dict) else None) or u_name
                    email = getattr(u, "email", None) or (u.get("email") if isinstance(u, dict) else None)
                    groups = list(getattr(u, "groups", None) or (u.get("groups") if isinstance(u, dict) else []) or [])
                    return {
                        "username": u_name,
                        "display_name": display_name,
                        "email": email,
                        "groups": groups,
                        "auth_method": "mock"
                    }
        return None
