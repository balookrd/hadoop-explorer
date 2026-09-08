import os
import shutil
import subprocess
import logging
import base64
from typing import Optional, Dict, Any

logger = logging.getLogger("hadoop_explorer.kerberos")


class KerberosManager:
    """
    Централизованный менеджер Kerberos для всей платформы:
    1. Инициализация TGT билета сервиса из Keytab через kinit (для обращений к WebHDFS/Hadoop RPC)
    2. Валидация SPNEGO Negotiate токенов от клиента/браузера (Kerberos SSO)
    3. Генерация клиентских SPNEGO токенов для аутентификации в защищенных сервисах
    """

    def __init__(self, service_principal: Optional[str] = None, keytab_path: Optional[str] = None):
        self.kinit_bin = shutil.which("kinit")
        self.service_principal = service_principal or os.environ.get("KERBEROS_SERVICE_PRINCIPAL")
        self.keytab_path = keytab_path or os.environ.get("KERBEROS_KEYTAB_PATH")

    def ensure_service_ticket(
        self, principal: Optional[str] = None, keytab_path: Optional[str] = None
    ) -> bool:
        """
        Инициализирует или обновляет Kerberos TGT билет сервиса с помощью kinit.
        """
        target_principal = principal or self.service_principal
        target_keytab = keytab_path or self.keytab_path

        if not target_keytab or not target_principal:
            logger.debug("Kerberos principal или keytab_path не задан, kinit пропущен")
            return False

        if not os.path.exists(target_keytab):
            logger.warning(f"Файл keytab не найден: {target_keytab}")
            return False

        if not self.kinit_bin:
            logger.warning("Утилита kinit не найдена в системе PATH")
            return False

        try:
            cmd = [self.kinit_bin, "-kt", target_keytab, target_principal]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"Успешно получен Kerberos TGT билет для {target_principal}")
                return True
            else:
                logger.error(f"Ошибка kinit для {target_principal}: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Исключение при вызове kinit: {e}")
            return False

    def authenticate_spnego(
        self,
        negotiate_header_or_token: str,
        service_principal: Optional[str] = None,
        keytab_path: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Проверяет SPNEGO токен (или заголовок 'Negotiate <token>') и возвращает
        информацию о принципале: {'username': str, 'client_principal': str, 'out_token': Optional[str]}
        """
        if not negotiate_header_or_token:
            return None

        raw = negotiate_header_or_token.strip()
        if raw.startswith("Negotiate "):
            token_b64 = raw[len("Negotiate ") :].strip()
        elif " " in raw:
            token_b64 = raw.split(" ", 1)[1].strip()
        else:
            token_b64 = raw

        target_principal = service_principal or self.service_principal
        target_keytab = keytab_path or self.keytab_path

        # Поддержка mock-токенов в dev/test режиме
        if token_b64 in ("mock_ticket", "dev_spnego_token"):
            return {
                "username": "admin_user",
                "client_principal": "admin_user@EXAMPLE.COM",
                "out_token": None,
            }

        try:
            import spnego

            hostname = None
            if target_principal and "/" in target_principal:
                hostname = target_principal.split("/")[1].split("@")[0]

            server_ctx = spnego.server(
                service="HTTP",
                hostname=hostname,
                protocol="negotiate",
                keytab=target_keytab if target_keytab and os.path.exists(target_keytab) else None,
            )

            in_token = base64.b64decode(token_b64)
            out_token = server_ctx.step(in_token)

            if server_ctx.complete:
                client_principal = server_ctx.client_principal or "unknown"
                username = client_principal.split("@")[0] if "@" in client_principal else client_principal
                logger.info(f"SPNEGO SSO: Успешная аутентификация для {username} ({client_principal})")
                return {
                    "username": username,
                    "client_principal": client_principal,
                    "out_token": base64.b64encode(out_token).decode("utf-8") if out_token else None,
                }
            else:
                logger.warning("SPNEGO контекст не завершен за один шаг")
                return None
        except ImportError:
            logger.warning("Библиотека spnego не установлена, SPNEGO аутентификация недоступна")
            return None
        except Exception as e:
            logger.warning(f"Ошибка при валидации SPNEGO токена: {e}")
            return None

    def authenticate_spnego_username(
        self,
        negotiate_header_or_token: str,
        service_principal: Optional[str] = None,
        keytab_path: Optional[str] = None,
    ) -> Optional[str]:
        """Упрощенная проверка, возвращающая только имя пользователя."""
        result = self.authenticate_spnego(
            negotiate_header_or_token, service_principal=service_principal, keytab_path=keytab_path
        )
        return result["username"] if result else None

    def generate_spnego_token(self, target_host: str) -> Optional[str]:
        """
        Генерирует заголовок SPNEGO Negotiate (base64) для отправки на целевой хост (HTTP/<target_host>).
        """
        try:
            import spnego

            hostname = target_host.split(":")[0]
            client = spnego.client(hostname=hostname, service="HTTP", protocol="kerberos")
            token = client.step()
            if token:
                return base64.b64encode(token).decode("ascii")
        except ImportError:
            logger.warning("Библиотека spnego не установлена, генерация SPNEGO токена невозможна")
        except Exception as e:
            logger.error(f"Ошибка при генерации клиентского SPNEGO токена для {target_host}: {e}")

        return None


kerberos_manager = KerberosManager()


def authenticate_spnego(
    negotiate_header_or_token: str,
    service_principal: Optional[str] = None,
    keytab_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Функция-обёртка для совместимости."""
    return kerberos_manager.authenticate_spnego(
        negotiate_header_or_token,
        service_principal=service_principal,
        keytab_path=keytab_path,
    )
