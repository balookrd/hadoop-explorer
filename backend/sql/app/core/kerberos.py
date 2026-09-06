import base64
import logging
from typing import Optional, Dict, Any
import spnego
from app.core.config import settings

logger = logging.getLogger("kerberos_auth")

def authenticate_spnego(negotiate_token_b64: str) -> Optional[Dict[str, Any]]:
    """
    Валидирует SPNEGO Kerberos токен из заголовка Authorization: Negotiate <token>.
    Использует серверный keytab или текущий кэш билетов.
    """
    cfg = settings.auth.kerberos
    if not cfg.enabled:
        return None

    try:
        in_token = base64.b64decode(negotiate_token_b64)
        
        # Инициализация контекста SPNEGO на стороне сервера
        server_ctx = spnego.server(
            service=cfg.service_principal or "HTTP",
            protocol="negotiate",
            keytab=cfg.keytab_file
        )

        out_token = server_ctx.step(in_token)

        if server_ctx.complete:
            client_principal = server_ctx.client_principal
            # Преобразуем "username@REALM" -> "username"
            username = client_principal.split("@")[0] if client_principal else "unknown"
            
            return {
                "username": username,
                "display_name": username,
                "email": f"{username}@{client_principal.split('@')[1].lower()}" if "@" in client_principal else None,
                "groups": [],  # Kerberos PAC или LDAP обогащение
                "auth_method": "kerberos",
                "out_token": base64.b64encode(out_token).decode("utf-8") if out_token else None
            }
        else:
            logger.warning("SPNEGO контекст не завершен за один шаг")
            return None

    except Exception as e:
        logger.error(f"Ошибка при валидации Kerberos SPNEGO токена: {e}")
        return None
