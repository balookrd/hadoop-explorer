import json
import logging
from typing import Dict, Any, Optional, List, Tuple
import anyio
import httpx
import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
from backend.common.core.circuit_breaker import circuit_breaker_registry

logger = logging.getLogger("livy_client")


def _create_kerberos_session() -> requests.Session:
    session = requests.Session()
    session.auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL, sanitize_mutual_error_response=False)
    return session


class LivyClient:
    """
    Клиент к Apache Livy REST API.
    Поддерживает Kerberos SPNEGO, impersonation (proxyUser) и управление сессиями/statements.
    """

    def __init__(self, base_url: str, auth_type: str = "none", use_ssl: bool = False):
        self.base_url = base_url.rstrip("/")
        self.auth_type = auth_type.lower()
        self.use_ssl = use_ssl
        self._kerberos_session: Optional[requests.Session] = None
        self._http_client: Optional[httpx.AsyncClient] = None

    def _get_kerberos_session(self) -> requests.Session:
        if self._kerberos_session is None:
            self._kerberos_session = _create_kerberos_session()
        return self._kerberos_session

    def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=30.0, verify=self.use_ssl)
        return self._http_client

    async def aclose(self):
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    def _sync_request(
        self, method: str, path: str, json_data: Optional[dict] = None, params: Optional[dict] = None
    ) -> dict:
        url = f"{self.base_url}{path}"
        session = self._get_kerberos_session()
        resp = session.request(
            method=method,
            url=url,
            json=json_data,
            params=params,
            headers={"X-Requested-By": "spark-explorer", "Content-Type": "application/json"},
            timeout=30.0,
            verify=self.use_ssl,
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    async def _request(
        self, method: str, path: str, json_data: Optional[dict] = None, params: Optional[dict] = None
    ) -> dict:
        cb = circuit_breaker_registry.get(
            name=f"spark:livy:{self.base_url}",
            failure_threshold=3,
            recovery_timeout=30.0,
        )

        async def _execute_livy_call():
            if self.auth_type == "kerberos":
                return await anyio.to_thread.run_sync(self._sync_request, method, path, json_data, params)

            client = self._get_http_client()
            url = f"{self.base_url}{path}"
            resp = await client.request(
                method=method,
                url=url,
                json=json_data,
                params=params,
                headers={"X-Requested-By": "spark-explorer", "Content-Type": "application/json"},
            )
            resp.raise_for_status()
            return resp.json() if resp.content else {}

        return await cb.call_async(_execute_livy_call)

    async def create_session(
        self,
        kind: str,
        proxy_user: Optional[str] = None,
        queue: Optional[str] = None,
        conf: Optional[Dict[str, str]] = None,
        jars: Optional[List[str]] = None,
        py_files: Optional[List[str]] = None,
        packages: Optional[List[str]] = None,
        archives: Optional[List[str]] = None,
        driver_memory: Optional[str] = None,
        driver_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
        executor_cores: Optional[int] = None,
        num_executors: Optional[int] = None,
        name: Optional[str] = None,
    ) -> dict:
        """
        Создает интерактивную сессию в Apache Livy.
        """
        payload: Dict[str, Any] = {
            "kind": kind,
        }
        if name:
            payload["name"] = name
        if proxy_user:
            payload["proxyUser"] = proxy_user
        if queue:
            payload["queue"] = queue
        if conf:
            payload["conf"] = conf
        if jars:
            payload["jars"] = [j for j in jars if j]
        if py_files:
            payload["pyFiles"] = [p for p in py_files if p]
        if packages:
            payload["packages"] = [p for p in packages if p]
        if archives:
            payload["archives"] = [a for a in archives if a]
        if driver_memory:
            payload["driverMemory"] = driver_memory
        if driver_cores:
            payload["driverCores"] = driver_cores
        if executor_memory:
            payload["executorMemory"] = executor_memory
        if executor_cores:
            payload["executorCores"] = executor_cores
        if num_executors:
            payload["numExecutors"] = num_executors

        logger.info(f"Создание Livy сессии: kind={kind}, proxyUser={proxy_user}, queue={queue}")
        return await self._request("POST", "/sessions", json_data=payload)

    async def get_session(self, session_id: int) -> dict:
        return await self._request("GET", f"/sessions/{session_id}")

    async def get_session_state(self, session_id: int) -> dict:
        return await self._request("GET", f"/sessions/{session_id}/state")

    async def delete_session(self, session_id: int) -> dict:
        return await self._request("DELETE", f"/sessions/{session_id}")

    async def execute_statement(self, session_id: int, code: str, kind: Optional[str] = None) -> dict:
        payload: Dict[str, Any] = {"code": code}
        if kind:
            payload["kind"] = kind
        return await self._request("POST", f"/sessions/{session_id}/statements", json_data=payload)

    async def get_statement(self, session_id: int, statement_id: int) -> dict:
        return await self._request("GET", f"/sessions/{session_id}/statements/{statement_id}")

    async def cancel_statement(self, session_id: int, statement_id: int) -> dict:
        return await self._request("POST", f"/sessions/{session_id}/statements/{statement_id}/cancel")

    async def get_session_log(self, session_id: int, from_line: Optional[int] = None, size: int = 50) -> dict:
        params: Dict[str, Any] = {"size": size}
        if from_line is not None:
            params["from"] = from_line
        return await self._request("GET", f"/sessions/{session_id}/log", params=params)
