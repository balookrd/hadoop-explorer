import ssl
import hmac
import hashlib
import logging
from typing import Optional, List, Dict, Any

import ldap3
from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.core.exceptions import LDAPException
from ldap3.utils.conv import escape_filter_chars

logger = logging.getLogger("hadoop_explorer.ldap")


class CommonLdapAuthService:
    """
    Общий сервис аутентификации LDAP/LDAPS и Mock пользователей.
    """

    @staticmethod
    def verify_mock_password(stored_password: str, input_password: str) -> bool:
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
                    return {
                        "username": u_name,
                        "display_name": getattr(u, "display_name", u_name) or u_name,
                        "email": getattr(u, "email", None),
                        "groups": list(getattr(u, "groups", []) or []),
                        "auth_method": "mock"
                    }
        return None
