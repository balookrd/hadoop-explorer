"""Модуль изоляции Kerberos-контекста для воркера и приемника.

Позволяет безопасно изолировать кэш билетов KRB5CCNAME для каждой отдельной задачи,
поддерживая как выполнение от системной техучетки, так и от учетных записей пользователей.
"""

import asyncio
import logging
import os
import shutil
import subprocess
from typing import Optional
import uuid

logger = logging.getLogger("replicator.security.kerberos")

DEFAULT_SERVICE_KEYTAB = os.environ.get("REPLICATOR_KEYTAB_PATH", "/etc/security/keytabs/replicator.keytab")
DEFAULT_SERVICE_PRINCIPAL = os.environ.get("REPLICATOR_SERVICE_PRINCIPAL", "hdfs-replicator@REALM.LOCAL")


class KerberosAuthenticationError(Exception):
    """Исключение при ошибке аутентификации в Kerberos KDC."""

    pass


class KerberosContextManager:
    """
    Контекстный менеджер для безопасной изоляции Kerberos-контекста (KRB5CCNAME).

    Поддерживает:
    - Аутентификацию системной техучетки по keytab (Proxy User)
    - Имперсонацию конкретного пользователя (doAs) для Apache Ranger аудита и применения политик
    - Изолированный файл кэша тикетов для предотвращения интерференции между задачами
    - Автоматическую очистку через kdestroy при выходе из контекста
    """

    def __init__(
        self,
        keytab: Optional[str] = None,
        principal: Optional[str] = None,
        run_as_service_account: bool = False,
        impersonate_user: Optional[str] = None,
        cache_dir: str = "/tmp",
        mock: Optional[bool] = None,
    ):
        self.run_as_service_account = run_as_service_account
        self.service_principal = os.environ.get("REPLICATOR_SERVICE_PRINCIPAL", DEFAULT_SERVICE_PRINCIPAL)
        self.service_keytab = keytab or DEFAULT_SERVICE_KEYTAB
        self.keytab = self.service_keytab

        # Вычисление пользователя для Hadoop doAs имперсонации
        if impersonate_user:
            self.impersonate_user: Optional[str] = impersonate_user
        elif principal and not run_as_service_account and principal != self.service_principal:
            # Извлекаем логин пользователя (например, 'alice@REALM.LOCAL' -> 'alice')
            self.impersonate_user = principal.split("@")[0].split("/")[0]
        else:
            self.impersonate_user = None

        # Идентификатор принципала для логирования и отслеживания контекста
        self.principal = principal or self.service_principal
        # Принципал, от имени которого выпускается Kerberos-тикет (техучетка Proxy User)
        self.auth_principal = self.service_principal

        self.cache_dir = cache_dir
        self.cache_file: Optional[str] = None
        self._prev_ccname: Optional[str] = None

        # Определение mock-режима (для сред тестирования и dev без KDC / keytab)
        if mock is not None:
            self.mock = mock
        else:
            self.mock = (
                os.environ.get("MOCK_KERBEROS", "false").lower() in ("true", "1", "yes")
                or shutil.which("kinit") is None
                or not os.path.isfile(self.keytab or "")
            )

    def __enter__(self) -> "KerberosContextManager":
        self._prev_ccname = os.environ.get("KRB5CCNAME")
        unique_id = uuid.uuid4().hex
        self.cache_file = os.path.join(self.cache_dir, f"krb5cc_repl_{unique_id}")

        logger.info(
            f"Инициализация Kerberos-контекста: техучетка='{self.auth_principal}', "
            f"doAs='{self.impersonate_user or 'нет'}', keytab='{self.keytab}', "
            f"кэш='{self.cache_file}', mock={self.mock}"
        )

        if self.mock:
            # Создаем маркерный файл кэша в mock-режиме
            os.makedirs(self.cache_dir, exist_ok=True)
            with open(self.cache_file, "w") as f:
                f.write(f"MOCK_TICKET_CACHE:{self.auth_principal}:doAs={self.impersonate_user or 'none'}")
        else:
            if not os.path.isfile(self.keytab):
                raise KerberosAuthenticationError(f"Keytab-файл не найден по пути: '{self.keytab}'")

            # В Kerberos аутентификацию по keytab проходит системная техучетка,
            # настроенная как доверенный hadoop.proxyuser в core-site.xml
            cmd = ["kinit", "-kt", self.keytab, self.auth_principal, "-c", self.cache_file]
            try:
                res = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )
                if res.returncode != 0:
                    raise KerberosAuthenticationError(
                        f"Команда kinit завершилась с ошибкой (код {res.returncode}): {res.stderr.strip()}"
                    )
            except Exception as e:
                if not isinstance(e, KerberosAuthenticationError):
                    raise KerberosAuthenticationError(f"Ошибка выполнения kinit: {str(e)}") from e
                raise

        # Устанавливаем изолированную переменную окружения для процесса
        os.environ["KRB5CCNAME"] = self.cache_file
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            # Восстанавливаем предыдущую переменную окружения KRB5CCNAME
            if self._prev_ccname is not None:
                os.environ["KRB5CCNAME"] = self._prev_ccname
            else:
                os.environ.pop("KRB5CCNAME", None)

            # Уничтожаем Kerberos тикет через kdestroy
            if self.cache_file and os.path.exists(self.cache_file):
                if not self.mock and shutil.which("kdestroy") is not None:
                    try:
                        subprocess.run(
                            ["kdestroy", "-c", self.cache_file],
                            capture_output=True,
                            timeout=5,
                            check=False,
                        )
                    except Exception as e:
                        logger.warning(f"Ошибка при вызове kdestroy: {e}")

                # Гарантированное удаление файла с диска
                try:
                    os.remove(self.cache_file)
                except OSError as e:
                    logger.warning(f"Не удалось удалить файл кэша тикетов '{self.cache_file}': {e}")
        finally:
            logger.info(
                f"Kerberos-контекст для principal='{self.principal}' "
                f"(doAs='{self.impersonate_user or 'нет'}') успешно очищен"
            )

    async def __aenter__(self) -> "KerberosContextManager":
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.__enter__)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.__exit__, exc_type, exc_val, exc_tb)


def open_hdfs_stream_with_kerberos(
    hdfs_host: str,
    hdfs_port: int,
    hdfs_path: str,
    keytab: Optional[str] = None,
    principal: Optional[str] = None,
    run_as_service_account: bool = False,
    impersonate_user: Optional[str] = None,
    mock: Optional[bool] = None,
):
    """
    Безопасное открытие файла в HDFS через PyArrow с изоляцией Kerberos и doAs-имперсонацией.

    - Системная техучетка получает TGT через KerberosContextManager.
    - В HadoopFileSystem передается doAs-пользователь (user) для проверки политик в Apache Ranger
      и фиксации в audit log.
    """
    with KerberosContextManager(
        keytab=keytab,
        principal=principal,
        run_as_service_account=run_as_service_account,
        impersonate_user=impersonate_user,
        mock=mock,
    ) as krb_ctx:
        try:
            import pyarrow.fs as pafs

            # Передаем имперсонированного пользователя и явный путь к билету кэша
            hdfs = pafs.HadoopFileSystem(
                hdfs_host,
                port=hdfs_port,
                user=krb_ctx.impersonate_user,
                kerb_ticket=krb_ctx.cache_file,
            )
            return hdfs.open_input_stream(hdfs_path)
        except Exception as e:
            logger.warning(f"PyArrow HDFS недоступен в текущем окружении: {e}")
            return None
