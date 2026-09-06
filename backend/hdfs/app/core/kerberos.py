import os
import shutil
import subprocess
import logging
from typing import Optional, Tuple
from app.core.config import settings
from app.models.auth import UserInfo
from app.core.acl import is_global_admin

logger = logging.getLogger(__name__)


class KerberosManager:
    """
    Менеджер Kerberos:
    1. Инициализация TGT билета сервиса из Keytab для обращений к WebHDFS NameNode
    2. Валидация SPNEGO Negotiate токенов от браузера (если включен kerberos_sso)
    """

    def __init__(self):
        self.kinit_bin = shutil.which("kinit")

    def ensure_service_ticket(self, principal: str, keytab_path: str) -> bool:
        """
        Инициализирует или обновляет Kerberos TGT билет сервиса с помощью kinit.
        """
        if not keytab_path or not principal:
            logger.warning("Kerberos principal или keytab_path не задан")
            return False

        if not os.path.exists(keytab_path):
            logger.error(f"Файл keytab не найден: {keytab_path}")
            return False

        if not self.kinit_bin:
            logger.warning("Утилита kinit не найдена в системе PATH")
            return False

        try:
            cmd = [self.kinit_bin, "-kt", keytab_path, principal]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"Успешно получен Kerberos билет для {principal}")
                return True
            else:
                logger.error(f"Ошибка kinit для {principal}: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Исключение при вызове kinit: {e}")
            return False

    def authenticate_spnego(self, negotiate_header: str) -> Optional[str]:
        """
        Проверяет заголовок Authorization: Negotiate <token> от браузера
        и возвращает kerberos principal пользователя (username@REALM).
        """
        if not settings.kerberos_sso.enabled:
            return None

        if not negotiate_header.startswith("Negotiate "):
            return None

        in_token_b64 = negotiate_header[len("Negotiate "):].strip()

        try:
            import spnego
            context = spnego.server(
                service="HTTP",
                hostname=settings.kerberos_sso.service_principal.split("/")[1].split("@")[0] if "/" in settings.kerberos_sso.service_principal else None,
                protocol="kerberos",
                keytab=settings.kerberos_sso.keytab_path if settings.kerberos_sso.keytab_path else None
            )
            import base64
            in_token = base64.b64decode(in_token_b64)
            context.step(in_token)

            if context.complete:
                client_name = context.client_principal
                # Извлечение короткого имени (username из username@REALM)
                username = client_name.split("@")[0] if "@" in client_name else client_name
                logger.info(f"Успешная SPNEGO аутентификация для: {username} ({client_name})")
                return username
        except ImportError:
            logger.warning("Библиотека spnego не установлена, SPNEGO SSO отключен")
        except Exception as e:
            logger.error(f"Ошибка при валидации SPNEGO токена: {e}")

    def generate_spnego_token(self, target_host: str) -> Optional[str]:
        """
        Генерирует заголовок SPNEGO Negotiate (base64) для отправки на целевой хост (HTTP/<target_host>).
        """
        try:
            import spnego
            import base64

            # Извлекаем хост без порта
            hostname = target_host.split(":")[0]
            client = spnego.client(
                hostname=hostname,
                service="HTTP",
                protocol="kerberos"
            )
            token = client.step()
            if token:
                return base64.b64encode(token).decode("ascii")
        except ImportError:
            logger.warning("Библиотека spnego не установлена, генерация SPNEGO токена невозможна")
        except Exception as e:
            logger.error(f"Ошибка при генерации клиентского SPNEGO токена для {target_host}: {e}")

        return None


kerberos_manager = KerberosManager()
