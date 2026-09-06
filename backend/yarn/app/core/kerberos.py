import os
import shutil
import subprocess
import logging
from typing import Optional

from app.core.config import settings
from app.models.auth import UserSession, Role

logger = logging.getLogger(__name__)


class KerberosManager:
    def __init__(self):
        self.kinit_bin = shutil.which("kinit")
        self.config = settings.auth.kerberos

    def ensure_service_ticket(self, principal: Optional[str] = None, keytab_path: Optional[str] = None) -> bool:
        principal = principal or self.config.service_principal
        keytab_path = keytab_path or self.config.keytab_file

        if not keytab_path or not principal:
            logger.debug("Kerberos keytab или principal не задан, пропускаем kinit")
            return False

        if not os.path.exists(keytab_path):
            logger.warning(f"Kerberos keytab файл не найден: {keytab_path}")
            return False

        if not self.kinit_bin:
            logger.warning("Утилита kinit не найдена в системе PATH")
            return False

        try:
            cmd = [self.kinit_bin, "-kt", keytab_path, principal]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"Успешно получен Kerberos TGT билет для {principal}")
                return True
            else:
                logger.error(f"Ошибка kinit для {principal}: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Исключение при вызове kinit: {e}")
            return False

    def authenticate_spnego(self, negotiate_header: str) -> Optional[UserSession]:
        if not self.config.enabled:
            return None

        if not negotiate_header.startswith("Negotiate "):
            return None

        in_token_b64 = negotiate_header.split(" ", 1)[1].strip()

        try:
            import spnego
            server_context = spnego.server(
                service="HTTP",
                hostname=None,
                protocol="negotiate"
            )
            import base64
            in_token = base64.b64decode(in_token_b64)
            server_context.step(in_token)

            if server_context.complete:
                full_principal = server_context.client_principal
                username = full_principal.split("@")[0] if "@" in full_principal else full_principal
                logger.info(f"SPNEGO SSO: Успешная аутентификация для {username}")
                return UserSession(
                    username=username,
                    display_name=username,
                    groups=[],
                    auth_method="kerberos",
                    is_admin=False,
                    system_role=Role.READER
                )
        except Exception as e:
            logger.warning(f"Ошибка SPNEGO аутентификации: {e}")

        return None


kerberos_manager = KerberosManager()
