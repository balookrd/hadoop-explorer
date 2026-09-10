import asyncio
import base64
import logging
from typing import Optional, Dict, Any
import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


class AwxClientError(Exception):
    """Базовое исключение для ошибок взаимодействия с AWX."""

    pass


class AwxJobFailedError(AwxClientError):
    """Исключение при неудачном завершении джоба в AWX."""

    def __init__(self, job_id: int, status: str, stdout: str):
        super().__init__(f"AWX Job #{job_id} failed with status '{status}'")
        self.job_id = job_id
        self.status = status
        self.stdout = stdout


class AwxClient:
    """
    Асинхронный клиент для взаимодействия с REST API Ansible AWX / Red Hat Automation Platform.
    Обеспечивает запуск Job Template с передачей параметров в Base64 extra_vars,
    отслеживание статуса исполнения и получение stdout логов.
    """

    TERMINAL_STATES = {"successful", "failed", "error", "canceled"}

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        verify_ssl: Optional[bool] = None,
        timeout_seconds: Optional[int] = None,
        poll_interval_seconds: Optional[int] = None,
    ):
        self.base_url = (base_url or settings.awx.base_url or "").rstrip("/")
        self.token = token if token is not None else settings.awx.token
        self.verify_ssl = verify_ssl if verify_ssl is not None else settings.awx.verify_ssl
        self.timeout_seconds = timeout_seconds or settings.awx.timeout_seconds or 180
        self.poll_interval = poll_interval_seconds or settings.awx.poll_interval_seconds or 2

        # Проверка на mock-режим: включен ли auth.mode == "mock" либо mock url/токен
        self.is_mock = (
            not self.base_url
            or self.base_url.startswith("mock://")
            or self.token == "mock"
            or (settings.auth.mode == "mock" and (not self.token or self.base_url == "https://awx.company.local"))
        )

    def _get_headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    @staticmethod
    def encode_xml(xml_content: str) -> str:
        """Кодирует XML в строку Base64 для безопасной передачи через JSON extra_vars."""
        return base64.b64encode(xml_content.encode("utf-8")).decode("ascii")

    async def launch_job(
        self,
        job_template_id: int,
        xml_content: str,
        applied_by: str,
        cluster_id: str,
        change_request_id: Optional[int] = None,
        extra_vars_override: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Запускает Job Template в AWX с передачей Base64-кодированного XML и метаданных.
        Возвращает ID запущенного Job.
        """
        xml_b64 = self.encode_xml(xml_content)
        extra_vars: Dict[str, Any] = {
            "capacity_scheduler_xml_b64": xml_b64,
            "applied_by": applied_by,
            "cluster_id": cluster_id,
            "change_request_id": str(change_request_id) if change_request_id is not None else "manual",
        }
        if extra_vars_override:
            extra_vars.update(extra_vars_override)

        if self.is_mock:
            logger.info(
                f"[MOCK AWX] Запуск Job Template #{job_template_id} для кластера '{cluster_id}' "
                f"пользователем '{applied_by}', CR #{change_request_id}"
            )
            # Возвращаем синтетический ID для мок-тестов
            return 90001

        url = f"{self.base_url}/api/v2/job_templates/{job_template_id}/launch/"
        payload = {"extra_vars": extra_vars}

        try:
            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                resp = await client.post(
                    url,
                    headers=self._get_headers(),
                    json=payload,
                    timeout=15.0,
                )
                if resp.status_code not in (200, 201):
                    logger.error(f"AWX launch error ({resp.status_code}): {resp.text}")
                    raise AwxClientError(
                        f"Не удалось запустить Job Template #{job_template_id} в AWX: HTTP {resp.status_code} - {resp.text}"
                    )
                data = resp.json()
                job_id = data.get("job") or data.get("id")
                if not job_id:
                    raise AwxClientError(f"AWX не вернул job ID: {data}")
                logger.info(f"Успешно запущен AWX Job #{job_id} из Job Template #{job_template_id}")
                return int(job_id)
        except httpx.RequestError as e:
            logger.error(f"Сетевая ошибка при обращении к AWX ({url}): {e}")
            raise AwxClientError(f"Сетевая ошибка при вызове AWX: {e}")

    async def get_job_status(self, job_id: int) -> Dict[str, Any]:
        """Получает текущую информацию и статус задачи в AWX."""
        if self.is_mock:
            return {
                "id": job_id,
                "status": "successful",
                "started": "2026-09-09T10:00:00Z",
                "finished": "2026-09-09T10:00:15Z",
                "elapsed": 15.0,
            }

        url = f"{self.base_url}/api/v2/jobs/{job_id}/"
        try:
            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                resp = await client.get(
                    url,
                    headers=self._get_headers(),
                    timeout=10.0,
                )
                if resp.status_code != 200:
                    raise AwxClientError(f"Ошибка получения статуса Job #{job_id}: HTTP {resp.status_code}")
                return resp.json()
        except httpx.RequestError as e:
            raise AwxClientError(f"Сетевая ошибка при получении статуса Job #{job_id}: {e}")

    async def get_job_stdout(self, job_id: int) -> str:
        """Получает консольный вывод (stdout) выполнения задачи в AWX."""
        if self.is_mock:
            return (
                "PLAY [Deploy YARN Capacity Scheduler via AWX] *************************************\n"
                "TASK [yarn_capacity_scheduler : Pre-flight XML validation on Ansible Controller] **\n"
                "ok: [localhost]\n"
                "TASK [yarn_capacity_scheduler : Execute yarn rmadmin -refreshQueues] **************\n"
                "changed: [rm1.company.local]\n"
                "PLAY RECAP *********************************************************************\n"
                "rm1.company.local          : ok=5    changed=2    unreachable=0    failed=0\n"
                "[SUCCESS] YARN queues refreshed on Active RM."
            )

        url = f"{self.base_url}/api/v2/jobs/{job_id}/stdout/?format=txt"
        try:
            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                resp = await client.get(
                    url,
                    headers=self._get_headers(),
                    timeout=15.0,
                )
                return resp.text if resp.status_code == 200 else ""
        except httpx.RequestError:
            return ""

    async def wait_for_job(
        self,
        job_id: int,
        timeout_seconds: Optional[int] = None,
        poll_interval: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Ожидает завершения задачи в AWX с заданным таймаутом и интервалом опроса.
        В случае неуспеха возбуждает AwxJobFailedError.
        """
        timeout = timeout_seconds or self.timeout_seconds
        interval = poll_interval or self.poll_interval

        if self.is_mock:
            stdout = await self.get_job_stdout(job_id)
            return {
                "job_id": job_id,
                "status": "successful",
                "stdout": stdout,
                "finished": "2026-09-09T10:00:15Z",
            }

        elapsed = 0.0
        while elapsed < timeout:
            job_info = await self.get_job_status(job_id)
            status = job_info.get("status", "unknown")

            if status in self.TERMINAL_STATES:
                stdout = await self.get_job_stdout(job_id)
                if status != "successful":
                    raise AwxJobFailedError(job_id=job_id, status=status, stdout=stdout)
                return {
                    "job_id": job_id,
                    "status": status,
                    "stdout": stdout,
                    "finished": job_info.get("finished"),
                }

            await asyncio.sleep(interval)
            elapsed += interval

        raise AwxClientError(f"Превышено время ожидания завершения Job #{job_id} в AWX ({timeout}s)")
