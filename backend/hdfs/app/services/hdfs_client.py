import io
import time
import logging
import json
import re
import ipaddress
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional, AsyncIterator, Tuple, Union
import httpx

from app.core.config import settings
from app.models.cluster import ClusterConfig
from app.models.hdfs import HdfsFileStatus, DirectoryListingResponse
from backend.common.core.kerberos import kerberos_manager
from backend.common.core.circuit_breaker import circuit_breaker_registry, CircuitBreakerOpenException
from backend.common.core.retry import retry_async

logger = logging.getLogger(__name__)

BLOCKED_METADATA_HOSTS = {
    "169.254.169.254",
    "metadata.google.internal",
    "metadata",
    "instance-data",
    "100.100.100.200",  # Alibaba Cloud metadata
    "169.254.170.2",  # AWS ECS metadata
}


def is_trusted_redirect_host(location: str, cluster: Optional[ClusterConfig] = None) -> bool:
    """
    Проверяет, принадлежит ли хост Location доверенному домену кластера перед отправкой hadoop.auth cookie (CWE-200).
    """
    if not cluster:
        return True
    try:
        parsed = urlparse(location)
        loc_host = parsed.hostname.lower() if parsed.hostname else ""
        nn_host = urlparse(cluster.active_endpoint).hostname.lower() if cluster.active_endpoint else ""
        if not nn_host:
            return True
        if loc_host == nn_host:
            return True
        nn_parts = nn_host.split(".", 1)
        if len(nn_parts) > 1 and loc_host.endswith("." + nn_parts[1]):
            return True
        return False
    except Exception:
        return False


def validate_webhdfs_location(location: str, cluster: Optional[ClusterConfig] = None) -> str:
    """
    Валидация Location URL для предотвращения SSRF атак через редиректы WebHDFS (CWE-918).
    Проверяет схему (http/https), блокирует доступ к Cloud Metadata (169.254.169.254,
    metadata.google.internal и др.), loopback/локальным адресам и выполняет DNS-резолвинг.
    """
    if not location or not isinstance(location, str):
        raise WebHdfsException("Некорректный заголовок Location от WebHDFS", 400)

    try:
        parsed = urlparse(location)
    except Exception as e:
        raise WebHdfsException(f"Некорректный URL в заголовке Location: {e}", 400)

    if parsed.scheme.lower() not in ("http", "https"):
        raise WebHdfsException(f"Недопустимая схема URL в Location: {parsed.scheme}", 400)

    hostname = parsed.hostname
    if not hostname:
        raise WebHdfsException("В Location URL отсутствует имя хоста", 400)

    hostname_lower = hostname.lower()

    # 1. Блокировка известных эндпоинтов Cloud Metadata
    if hostname_lower in BLOCKED_METADATA_HOSTS or hostname_lower.endswith(".internal.cloudapp.net"):
        raise WebHdfsException(f"Запрещенный адрес назначения (SSRF Protection): {hostname}", 403)

    is_dev = settings.server.debug or (cluster and getattr(cluster, "mock_storage", False))

    # 2. Проверка IP адресов (link-local, cloud metadata ranges, loopback)
    try:
        ip = ipaddress.ip_address(hostname_lower)
        if ip.is_link_local:
            raise WebHdfsException(f"Доступ к link-local адресам запрещен (SSRF Protection): {hostname}", 403)

        if ip.is_loopback and not is_dev:
            raise WebHdfsException(
                f"Доступ к loopback адресам запрещен в production (SSRF Protection): {hostname}", 403
            )

        if ip.is_unspecified:
            raise WebHdfsException(f"Недопустимый IP адрес: {hostname}", 403)

    except ValueError:
        # Доменное имя: DNS-резолвинг и проверка полученных IP адресов (защита от DNS Rebinding)
        if (hostname_lower == "localhost" or hostname_lower.endswith(".localhost")) and not is_dev:
            raise WebHdfsException(f"Доступ к localhost запрещен в production (SSRF Protection): {hostname}", 403)

        if not is_dev:
            try:
                import socket

                addr_info = socket.getaddrinfo(hostname_lower, None)
                for family, socktype, proto, canonname, sockaddr in addr_info:
                    ip_str = sockaddr[0]
                    resolved_ip = ipaddress.ip_address(ip_str)
                    if (
                        resolved_ip.is_link_local
                        or resolved_ip.is_loopback
                        or resolved_ip.is_unspecified
                        or resolved_ip.is_reserved
                        or ip_str in BLOCKED_METADATA_HOSTS
                    ):
                        raise WebHdfsException(
                            f"Доменное имя разрешается в запрещенный IP адрес {ip_str} (SSRF / DNS Rebinding Protection): {hostname}",
                            403,
                        )
            except socket.gaierror:
                pass  # Разрешение не удалось, httpx обработает ошибку соединения

    return location


def parse_hdfs_error(error_raw: Any, status_code: int = 500) -> Tuple[str, str]:
    """
    Преобразует технические ошибки WebHDFS (RemoteException, Java stacktraces)
    в понятные сообщения для пользователя на русском языке.
    Возвращает кортеж: (понятное_сообщение, класс_исключения).
    """
    raw_str = ""
    if isinstance(error_raw, bytes):
        try:
            raw_str = error_raw.decode("utf-8", errors="replace")
        except Exception:
            raw_str = str(error_raw)
    elif isinstance(error_raw, dict):
        raw_str = ""
    else:
        raw_str = str(error_raw)

    exception_class = ""
    message = ""

    # 1. Попытка распарсить как JSON ({"RemoteException": {...}})
    parsed_json = None
    if isinstance(error_raw, dict):
        parsed_json = error_raw
    elif raw_str:
        start_idx = raw_str.find("{")
        end_idx = raw_str.rfind("}")
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            try:
                parsed_json = json.loads(raw_str[start_idx : end_idx + 1])
            except Exception:
                parsed_json = None

    if parsed_json and isinstance(parsed_json, dict):
        remote_ex = parsed_json.get("RemoteException", parsed_json)
        if isinstance(remote_ex, dict):
            exception_class = remote_ex.get("exception", "")
            message = remote_ex.get("message", "")

    # Если JSON не распарсился, берем исходную строку
    if not message and raw_str:
        message = raw_str

    # 2. Очищаем сообщение от Java-стектрейса (\tat ... и переносов строк)
    clean_lines = [line.strip() for line in message.replace("\r", "\n").split("\n") if line.strip()]
    first_meaningful_line = ""
    for line in clean_lines:
        if line.startswith("at ") or line.startswith("\tat "):
            continue
        first_meaningful_line = line
        break

    if not first_meaningful_line and clean_lines:
        first_meaningful_line = clean_lines[0]

    # Убираем префиксы типа "org.apache.hadoop.security.AccessControlException: "
    if ": " in first_meaningful_line:
        prefix, rest = first_meaningful_line.split(": ", 1)
        if "." in prefix and "Exception" in prefix:
            if not exception_class:
                exception_class = prefix.split(".")[-1]
            first_meaningful_line = rest

    # 3. Интеллектуальный перевод типовых ошибок HDFS на русский язык
    line_lower = first_meaningful_line.lower()

    # AccessControlException / Permission denied
    if "accesscontrol" in exception_class.lower() or "permission denied" in line_lower:
        match = re.search(
            r'user=(?P<user>[^,\s]+).*?access=(?P<access>[^,\s]+).*?inode="?(?P<inode>[^":\s]+)"?',
            first_meaningful_line,
        )
        if match:
            u = match.group("user")
            acc = match.group("access")
            path = match.group("inode")
            access_ru = {
                "READ": "чтение",
                "WRITE": "запись",
                "EXECUTE": "доступ/выполнение",
                "READ_EXECUTE": "чтение и доступ",
                "ALL": "полный доступ",
            }.get(acc.upper(), acc)
            return (
                f"Отказано в доступе: у пользователя '{u}' нет прав на {access_ru} для '{path}'.",
                "AccessControlException",
            )

        return (f"Отказано в доступе (Permission denied): {first_meaningful_line}", "AccessControlException")

    # FileAlreadyExistsException
    if "filealreadyexists" in exception_class.lower() or "already exists" in line_lower:
        return (f"Файл или директория уже существует: {first_meaningful_line}", "FileAlreadyExistsException")

    # FileNotFoundException
    if (
        "filenotfound" in exception_class.lower()
        or "file does not exist" in line_lower
        or "does not exist" in line_lower
    ):
        return (f"Файл или директория не найдена: {first_meaningful_line}", "FileNotFoundException")

    # PathIsNotEmptyDirectoryException
    if "pathisnotempty" in exception_class.lower() or "is not empty" in line_lower:
        return (
            "Каталог не пуст. Включите опцию рекурсивного удаления, чтобы удалить его вместе с содержимым.",
            "PathIsNotEmptyDirectoryException",
        )

    # SafeModeException
    if "safemode" in exception_class.lower() or "safe mode" in line_lower:
        return (
            "Кластер HDFS находится в безопасном режиме (SafeMode). Запись временно заблокирована.",
            "SafeModeException",
        )

    # StandbyException
    if "standby" in exception_class.lower():
        return ("NameNode находится в режиме ожидания (Standby). Запись невозможна.", "StandbyException")

    # QuotaExceededException
    if "quotaexceeded" in exception_class.lower() or "quota exceeded" in line_lower:
        return (f"Превышена квота HDFS: {first_meaningful_line}", "QuotaExceededException")

    # 4. HTTP статусы по умолчанию
    if status_code == 401:
        return (
            "Ошибка аутентификации (401 Unauthorized). Проверьте учетные данные или Kerberos тикет.",
            "Unauthorized",
        )
    if status_code == 403:
        return (f"Доступ запрещен (403 Forbidden): {first_meaningful_line or 'Недостаточно прав'}", "Forbidden")
    if status_code == 404:
        return (f"Ресурс не найден (404 Not Found): {first_meaningful_line or 'Путь не существует'}", "NotFound")

    clean_res = first_meaningful_line if first_meaningful_line else "Ошибка файловой системы HDFS"
    return (clean_res, exception_class or "HdfsError")


class WebHdfsException(Exception):
    def __init__(self, raw_error: Any, status_code: int = 500, exception_class: str = ""):
        clean_msg, parsed_class = parse_hdfs_error(raw_error, status_code)
        self.message = clean_msg
        self.raw_error = str(raw_error)
        self.status_code = status_code
        self.exception_class = exception_class or parsed_class
        super().__init__(self.message)


class MockStorage:
    """
    Эмулятор файловой системы HDFS для демонстрации и локального тестирования.
    """

    def __init__(self):
        now = int(time.time() * 1000)
        self.files: Dict[str, Dict[str, Any]] = {
            "/": {
                "type": "DIRECTORY",
                "length": 0,
                "owner": "hdfs",
                "group": "supergroup",
                "permission": "755",
                "modificationTime": now,
                "accessTime": now,
            },
            "/user": {
                "type": "DIRECTORY",
                "length": 0,
                "owner": "hdfs",
                "group": "supergroup",
                "permission": "755",
                "modificationTime": now,
                "accessTime": now,
            },
            "/tmp": {
                "type": "DIRECTORY",
                "length": 0,
                "owner": "hdfs",
                "group": "supergroup",
                "permission": "777",
                "modificationTime": now,
                "accessTime": now,
            },
            "/data": {
                "type": "DIRECTORY",
                "length": 0,
                "owner": "hdfs",
                "group": "hadoop-admins",
                "permission": "750",
                "modificationTime": now,
                "accessTime": now,
            },
            "/data/events.json": {
                "type": "FILE",
                "length": 142,
                "owner": "engineer",
                "group": "data-engineers",
                "permission": "644",
                "modificationTime": now,
                "accessTime": now,
                "content": b'{"event_id": 1001, "name": "user_signup", "timestamp": "2026-09-05T09:00:00Z"}\n{"event_id": 1002, "name": "page_view", "timestamp": "2026-09-05T09:01:15Z"}\n',
            },
            "/data/metrics.csv": {
                "type": "FILE",
                "length": 128,
                "owner": "analyst",
                "group": "analytics",
                "permission": "644",
                "modificationTime": now,
                "accessTime": now,
                "content": b"date,cpu_usage,memory_mb,requests_sec\n2026-09-01,24.5,4096,1520\n2026-09-02,31.2,4200,1890\n2026-09-03,28.0,4150,1670\n2026-09-04,45.1,5120,2400\n",
            },
            "/tmp/readme.txt": {
                "type": "FILE",
                "length": 78,
                "owner": "admin",
                "group": "hadoop-admins",
                "permission": "644",
                "modificationTime": now,
                "accessTime": now,
                "content": b"Welcome to HDFS Explorer Mock Storage.\nAll file operations are supported here!\n",
            },
        }

    def _ensure_user_home(self, username: str):
        user_home = f"/user/{username}"
        if user_home not in self.files:
            now = int(time.time() * 1000)
            self.files[user_home] = {
                "type": "DIRECTORY",
                "length": 0,
                "owner": username,
                "group": "domain users",
                "permission": "700",
                "modificationTime": now,
                "accessTime": now,
            }
            welcome_file = f"{user_home}/welcome.txt"
            self.files[welcome_file] = {
                "type": "FILE",
                "length": 85,
                "owner": username,
                "group": "domain users",
                "permission": "644",
                "modificationTime": now,
                "accessTime": now,
                "content": f"Hello {username}!\nThis is your personal HDFS home directory.\n".encode("utf-8"),
            }

    def list_status(self, path: str, username: str) -> List[HdfsFileStatus]:
        clean_path = "/" + path.strip("/")
        self._ensure_user_home(username)

        if clean_path not in self.files:
            raise WebHdfsException(f"Path does not exist: {clean_path}", 404)

        if self.files[clean_path]["type"] != "DIRECTORY":
            raise WebHdfsException(f"Path is not a directory: {clean_path}", 400)

        results = []
        prefix = clean_path if clean_path != "/" else ""

        for p, meta in self.files.items():
            if p == clean_path:
                continue
            # Проверяем, является ли p прямым потомком clean_path
            if p.startswith(prefix + "/"):
                remainder = p[len(prefix) + 1 :]
                if "/" not in remainder:  # Прямой потомок
                    results.append(
                        HdfsFileStatus(
                            pathSuffix=remainder,
                            type=meta["type"],
                            length=meta["length"],
                            owner=meta["owner"],
                            group=meta["group"],
                            permission=meta["permission"],
                            accessTime=meta["accessTime"],
                            modificationTime=meta["modificationTime"],
                            blockSize=134217728,
                            replication=3 if meta["type"] == "FILE" else 0,
                        )
                    )

        return sorted(results, key=lambda x: (x.type != "DIRECTORY", x.pathSuffix.lower()))

    def get_file_status(self, path: str, username: str) -> HdfsFileStatus:
        clean_path = "/" + path.strip("/")
        self._ensure_user_home(username)
        if clean_path not in self.files:
            raise WebHdfsException(f"Path does not exist: {clean_path}", 404)
        meta = self.files[clean_path]
        return HdfsFileStatus(
            pathSuffix=clean_path.split("/")[-1] if clean_path != "/" else "",
            type=meta["type"],
            length=meta["length"],
            owner=meta["owner"],
            group=meta["group"],
            permission=meta["permission"],
            accessTime=meta["accessTime"],
            modificationTime=meta["modificationTime"],
            blockSize=134217728,
            replication=3 if meta["type"] == "FILE" else 0,
        )

    def get_file_content(self, path: str, offset: int = 0, length: Optional[int] = None) -> bytes:
        clean_path = "/" + path.strip("/")
        if clean_path not in self.files:
            raise WebHdfsException(f"File not found: {clean_path}", 404)

        item = self.files[clean_path]
        if item["type"] == "DIRECTORY":
            raise WebHdfsException(f"Path is a directory: {clean_path}", 400)

        content: bytes = item.get("content", b"")
        if length is not None:
            return content[offset : offset + length]
        return content[offset:]

    def put_file(self, path: str, content: bytes, username: str, overwrite: bool = True):
        clean_path = "/" + path.strip("/")
        now = int(time.time() * 1000)
        self.files[clean_path] = {
            "type": "FILE",
            "length": len(content),
            "owner": username,
            "group": "domain users",
            "permission": "644",
            "modificationTime": now,
            "accessTime": now,
            "content": content,
        }

    def mkdirs(self, path: str, username: str):
        clean_path = "/" + path.strip("/")
        now = int(time.time() * 1000)
        self.files[clean_path] = {
            "type": "DIRECTORY",
            "length": 0,
            "owner": username,
            "group": "domain users",
            "permission": "755",
            "modificationTime": now,
            "accessTime": now,
        }

    def rename(self, src: str, dst: str):
        src_clean = "/" + src.strip("/")
        dst_clean = "/" + dst.strip("/")
        if src_clean not in self.files:
            raise WebHdfsException(f"Source does not exist: {src_clean}", 404)

        # Переименовываем файл или всё поддерево
        to_move = {}
        for p, meta in self.files.items():
            if p == src_clean or p.startswith(src_clean + "/"):
                new_p = dst_clean + p[len(src_clean) :]
                to_move[p] = (new_p, meta)

        for old_p in to_move:
            del self.files[old_p]
        for _, (new_p, meta) in to_move.items():
            self.files[new_p] = meta

    def delete(self, path: str, recursive: bool = False):
        clean_path = "/" + path.strip("/")
        if clean_path not in self.files:
            raise WebHdfsException(f"Path does not exist: {clean_path}", 404)

        to_delete = [p for p in self.files if p == clean_path or p.startswith(clean_path + "/")]
        if len(to_delete) > 1 and not recursive:
            raise WebHdfsException(f"Directory is not empty: {clean_path}", 400)

        for p in to_delete:
            del self.files[p]


class HdfsClient:
    """
    Клиент WebHDFS с поддержкой:
    1. Нескольких NameNode (HA failover)
    2. Имперсонации пользователя (doAs)
    3. Kerberos аутентификации сервиса
    4. Пулинга соединений и переиспользования httpx.AsyncClient
    """

    def __init__(self, cluster: ClusterConfig):
        self.cluster = cluster
        self.active_url_index = 0
        self.mock_storage = MockStorage() if cluster.mock_storage else None
        self._auth_cookies: Dict[str, str] = {}
        self._http_client: Optional[httpx.AsyncClient] = None

    def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            from backend.common.core.http_client import create_async_http_client

            self._http_client = create_async_http_client(
                timeout=float(self.cluster.timeout_seconds),
                max_keepalive=20,
                max_connections=50,
                keepalive_expiry=30.0,
                follow_redirects=False,
            )
        return self._http_client

    async def aclose(self):
        """Закрывает базовый пул HTTP-соединений."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None

    async def close(self):
        await self.aclose()

    @property
    def current_url(self) -> str:
        return self.cluster.webhdfs_urls[self.active_url_index].rstrip("/")

    def _switch_to_next_nn(self):
        if len(self.cluster.webhdfs_urls) > 1:
            self.active_url_index = (self.active_url_index + 1) % len(self.cluster.webhdfs_urls)
            logger.warning(f"Переключение на следующий WebHDFS NameNode: {self.current_url}")

    def _save_response_cookies(self, resp: httpx.Response):
        """
        Сохраняет cookie hadoop.auth, полученный после успешного Kerberos/SPNEGO рукопожатия.
        """
        for k, v in resp.cookies.items():
            if k == "hadoop.auth":
                self._auth_cookies[k] = v

    def _prepare_auth_and_params(
        self, params: Dict[str, Any], do_as_user: str, target_url: Optional[str] = None
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        req_params = dict(params)
        req_params["doAs"] = do_as_user
        headers: Dict[str, str] = {}

        if self.cluster.auth_type == "kerberos":
            if self.cluster.service_principal and self.cluster.keytab_path:
                kerberos_manager.ensure_service_ticket(self.cluster.service_principal, self.cluster.keytab_path)

            # Если есть сохраненный hadoop.auth cookie, подставляем его для быстрого доступа
            if "hadoop.auth" in self._auth_cookies:
                headers["Cookie"] = f"hadoop.auth={self._auth_cookies['hadoop.auth']}"
            else:
                # Генерируем Negotiate SPNEGO токен для целевого узла
                from urllib.parse import urlparse

                effective_url = target_url or self.current_url
                target_host = urlparse(effective_url).hostname or "localhost"
                token = kerberos_manager.generate_spnego_token(target_host)
                if token:
                    headers["Authorization"] = f"Negotiate {token}"
        else:
            req_params["user.name"] = "hdfs-explorer"

        return req_params, headers

    async def _execute_request(
        self,
        method: str,
        path: str,
        op: str,
        do_as_user: str,
        extra_params: Optional[Dict[str, Any]] = None,
        content: Optional[bytes] = None,
        stream: bool = False,
    ) -> httpx.Response:
        params = {"op": op}
        if extra_params:
            params.update(extra_params)

        clean_path = path.strip("/")

        # Пробуем доступные NameNodes
        last_exception = None
        for attempt in range(len(self.cluster.webhdfs_urls)):
            nn_url = self.current_url
            cb = circuit_breaker_registry.get(
                name=f"hdfs:{self.cluster.id}:{nn_url}",
                failure_threshold=3,
                recovery_timeout=20.0,
                excluded_exceptions=(WebHdfsException,),
            )

            try:
                cb.before_call()
            except CircuitBreakerOpenException as cbe:
                logger.info(f"NameNode {nn_url} временно недоступен ({cbe}), быстрый переход к следующему узлу.")
                self._switch_to_next_nn()
                continue

            url = f"{nn_url}/{clean_path}"
            req_params, headers = self._prepare_auth_and_params(params, do_as_user, url)
            try:

                async def _do_http_call():
                    client = self._get_http_client()
                    resp = await client.request(
                        method=method,
                        url=url,
                        params=req_params,
                        headers=headers,
                        content=content,
                        follow_redirects=False,
                    )

                    # Если 401 Unauthorized с Negotiate и кука устарела - сбрасываем и повторяем один раз
                    if resp.status_code == 401 and "hadoop.auth" in self._auth_cookies:
                        self._auth_cookies.pop("hadoop.auth", None)
                        retry_params, retry_headers = self._prepare_auth_and_params(params, do_as_user, url)
                        resp = await client.request(
                            method=method,
                            url=url,
                            params=retry_params,
                            headers=retry_headers,
                            content=content,
                            follow_redirects=False,
                        )

                    self._save_response_cookies(resp)

                    # Проверка на StandbyException (NameNode в режиме ожидания)
                    if resp.status_code == 403 and "StandbyException" in resp.text:
                        logger.info(f"NameNode {nn_url} в режиме STANDBY. Пробуем следующую.")
                        self._switch_to_next_nn()
                        return None

                    # Проверка ошибок WebHDFS
                    if resp.status_code >= 400:
                        raise WebHdfsException(resp.text, resp.status_code)

                    return resp

                async def _call_with_retry():
                    return await retry_async(
                        _do_http_call,
                        max_attempts=2,
                        initial_delay=0.2,
                        max_delay=1.0,
                        retry_exceptions=(httpx.ConnectError, httpx.TimeoutException),
                        operation_name=f"WebHDFS {method} {clean_path}",
                    )

                resp = await cb.call_async(_call_with_retry)
                if resp is not None:
                    return resp

            except (httpx.ConnectError, httpx.TimeoutException) as e:
                logger.warning(f"Ошибка подключения к {nn_url}: {e}")
                last_exception = e
                self._switch_to_next_nn()
            except WebHdfsException:
                raise
            except Exception as e:
                logger.warning(f"Непредвиденная ошибка при запросе к NameNode {nn_url}: {e}")
                last_exception = e
                self._switch_to_next_nn()

        raise WebHdfsException(f"Не удалось связаться ни с одной NameNode кластера: {last_exception}", 503)

    async def get_file_status(self, path: str, do_as_user: str) -> HdfsFileStatus:
        clean_path = "/" + path.strip("/")
        if self.mock_storage:
            return self.mock_storage.get_file_status(clean_path, do_as_user)

        resp = await self._execute_request("GET", clean_path, "GETFILESTATUS", do_as_user)
        data = resp.json()
        status_dict = data.get("FileStatus", {})
        return HdfsFileStatus(**status_dict)

    async def list_status(self, path: str, do_as_user: str) -> List[HdfsFileStatus]:
        if self.mock_storage:
            return self.mock_storage.list_status(path, do_as_user)

        resp = await self._execute_request("GET", path, "LISTSTATUS", do_as_user)
        data = resp.json()
        statuses = data.get("FileStatuses", {}).get("FileStatus", [])
        return [HdfsFileStatus(**s) for s in statuses]

    async def get_file_content(
        self, path: str, do_as_user: str, offset: int = 0, length: Optional[int] = None
    ) -> bytes:
        if self.mock_storage:
            return self.mock_storage.get_file_content(path, offset, length)

        params: Dict[str, Any] = {"offset": offset}
        if length is not None:
            params["length"] = length

        # WebHDFS при OPEN возвращает 307 Redirect на DataNode
        clean_path = path.strip("/")
        url = f"{self.current_url}/{clean_path}"
        req_params, headers = self._prepare_auth_and_params({"op": "OPEN", **params}, do_as_user, url)

        client = self._get_http_client()
        resp = await client.get(url, params=req_params, headers=headers)
        self._save_response_cookies(resp)
        if resp.status_code in (307, 302, 301):
            location = resp.headers.get("Location")
            if not location:
                raise WebHdfsException("NameNode не вернула заголовок Location для чтения файла", 500)
            validate_webhdfs_location(location, self.cluster)
            dn_headers = {}
            if "hadoop.auth" in self._auth_cookies and is_trusted_redirect_host(location, self.cluster):
                dn_headers["Cookie"] = f"hadoop.auth={self._auth_cookies['hadoop.auth']}"
            resp = await client.get(location, headers=dn_headers, follow_redirects=False)
            self._save_response_cookies(resp)

        if resp.status_code >= 400:
            raise WebHdfsException(resp.text, resp.status_code)
        return resp.content

    async def open_stream(self, path: str, do_as_user: str) -> AsyncIterator[bytes]:
        """
        Стриминговое скачивание большого файла.
        """
        if self.mock_storage:
            content = self.mock_storage.get_file_content(path)
            chunk_size = 65536
            for i in range(0, len(content), chunk_size):
                yield content[i : i + chunk_size]
            return

        clean_path = path.strip("/")
        url = f"{self.current_url}/{clean_path}"
        req_params, headers = self._prepare_auth_and_params({"op": "OPEN"}, do_as_user, url)

        client = self._get_http_client()
        # 1. Запрос на получение ссылки от NameNode
        init_resp = await client.get(url, params=req_params, headers=headers)
        self._save_response_cookies(init_resp)
        if init_resp.status_code in (307, 302, 301):
            location = init_resp.headers.get("Location")
            if not location:
                raise WebHdfsException("NameNode не вернула заголовок Location для стрима", 500)
            validate_webhdfs_location(location, self.cluster)
            target_url = location
            target_headers = {}
            if "hadoop.auth" in self._auth_cookies and is_trusted_redirect_host(location, self.cluster):
                target_headers["Cookie"] = f"hadoop.auth={self._auth_cookies['hadoop.auth']}"
            target_params = {}
        elif init_resp.status_code < 400:
            target_url = url
            target_headers = headers
            target_params = req_params
        else:
            raise WebHdfsException(init_resp.text, init_resp.status_code)

        async with client.stream("GET", target_url, params=target_params, headers=target_headers, timeout=None) as resp:
            self._save_response_cookies(resp)
            if resp.status_code >= 400:
                raise WebHdfsException(await resp.aread(), resp.status_code)
            async for chunk in resp.aiter_bytes():
                yield chunk

    async def create_file(
        self, path: str, content: Union[bytes, AsyncIterator[bytes]], do_as_user: str, overwrite: bool = True
    ):
        if self.mock_storage:
            if not isinstance(content, bytes):
                chunks = []
                async for chunk in content:
                    chunks.append(chunk)
                file_bytes = b"".join(chunks)
            else:
                file_bytes = content
            self.mock_storage.put_file(path, file_bytes, do_as_user, overwrite)
            return

        # Двухэтапный CREATE в WebHDFS:
        # 1. Запрос на NameNode с noredirect=true -> получение DataNode URL в заголовке Location
        clean_path = path.strip("/")
        url = f"{self.current_url}/{clean_path}"
        params = {"op": "CREATE", "overwrite": str(overwrite).lower()}
        req_params, headers = self._prepare_auth_and_params(params, do_as_user, url)

        client = self._get_http_client()
        resp1 = await client.put(url, params=req_params, headers=headers, follow_redirects=False)
        self._save_response_cookies(resp1)
        if resp1.status_code not in (307, 201):
            raise WebHdfsException(resp1.text, resp1.status_code)

        location = resp1.headers.get("Location")
        if not location:
            raise WebHdfsException("NameNode не вернула заголовок Location для загрузки файла", 500)

        validate_webhdfs_location(location, self.cluster)

        # 2. Потоковая отправка содержимого файла на полученный DataNode URL (без накопления в RAM)
        dn_headers = {"Content-Type": "application/octet-stream"}
        if "hadoop.auth" in self._auth_cookies and is_trusted_redirect_host(location, self.cluster):
            dn_headers["Cookie"] = f"hadoop.auth={self._auth_cookies['hadoop.auth']}"

        # Увеличенный таймаут для загрузки больших потоков данных
        stream_timeout = httpx.Timeout(600.0, connect=self.cluster.timeout_seconds)
        resp2 = await client.put(location, content=content, headers=dn_headers, timeout=stream_timeout)
        self._save_response_cookies(resp2)
        if resp2.status_code not in (200, 201):
            raise WebHdfsException(resp2.text, resp2.status_code)

    async def mkdirs(self, path: str, do_as_user: str):
        if self.mock_storage:
            self.mock_storage.mkdirs(path, do_as_user)
            return

        resp = await self._execute_request("PUT", path, "MKDIRS", do_as_user)
        data = resp.json()
        if not data.get("boolean", False):
            raise WebHdfsException(f"Не удалось создать директорию: {path}", 400)

    async def rename(self, src_path: str, dst_path: str, do_as_user: str):
        if self.mock_storage:
            self.mock_storage.rename(src_path, dst_path)
            return

        clean_dst = "/" + dst_path.strip("/")
        resp = await self._execute_request(
            "PUT", src_path, "RENAME", do_as_user, extra_params={"destination": clean_dst}
        )
        data = resp.json()
        if not data.get("boolean", False):
            raise WebHdfsException(f"Не удалось переименовать: {src_path} -> {dst_path}", 400)

    async def delete(self, path: str, do_as_user: str, recursive: bool = False):
        if self.mock_storage:
            self.mock_storage.delete(path, recursive)
            return

        resp = await self._execute_request(
            "DELETE", path, "DELETE", do_as_user, extra_params={"recursive": str(recursive).lower()}
        )
        data = resp.json()
        if not data.get("boolean", False):
            raise WebHdfsException(f"Не удалось удалить: {path}", 400)


class HdfsService:
    def __init__(self):
        self._clients: Dict[str, HdfsClient] = {}

    def get_client(self, cluster: ClusterConfig) -> HdfsClient:
        if cluster.id not in self._clients:
            self._clients[cluster.id] = HdfsClient(cluster)
        return self._clients[cluster.id]

    async def aclose(self):
        """Закрывает все клиенты и освобождает пулы соединений."""
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()

    async def close(self):
        await self.aclose()

    async def copy_cross_cluster(
        self,
        source_cluster: ClusterConfig,
        source_path: str,
        target_cluster: ClusterConfig,
        target_path: str,
        username: str,
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        """
        Копирует файл или директорию из одного кластера в другой с сохранением структуры.
        Включает защиту от зацикливания, потоковую передачу данных чанками и логирование прогресса.
        """
        source_client = self.get_client(source_cluster)
        target_client = self.get_client(target_cluster)

        clean_src = "/" + source_path.strip("/")
        clean_dst = "/" + target_path.strip("/") if target_path.strip("/") else "/"

        # Защита от зацикливания и некорректных путей в одном кластере
        if source_cluster.id == target_cluster.id:
            if clean_src == clean_dst:
                raise WebHdfsException("Исходный и целевой путь в одном кластере совпадают", 400)
            if clean_dst.startswith(clean_src.rstrip("/") + "/"):
                raise WebHdfsException(
                    "Целевой путь является подкаталогом исходного пути (защита от зацикливания)", 400
                )

        logger.info(
            f"Начало межкластерного копирования: '{source_cluster.id}:{clean_src}' -> "
            f"'{target_cluster.id}:{clean_dst}', пользователь='{username}', overwrite={overwrite}"
        )

        # Получаем статус исходного объекта
        src_status = await source_client.get_file_status(clean_src, username)
        src_name = clean_src.split("/")[-1] if clean_src != "/" else "root"

        copied_files = 0
        copied_bytes = 0

        if src_status.type == "FILE":
            dst_file_path = clean_dst
            try:
                dst_status = await target_client.get_file_status(clean_dst, username)
                if dst_status.type == "DIRECTORY":
                    dst_file_path = f"{clean_dst.rstrip('/')}/{src_name}"
            except WebHdfsException:
                if target_path.endswith("/"):
                    dst_file_path = f"{clean_dst.rstrip('/')}/{src_name}"

            logger.info(f"Копирование файла: '{clean_src}' -> '{dst_file_path}' ({src_status.length} байт)")
            file_stream = source_client.open_stream(clean_src, username)
            await target_client.create_file(dst_file_path, file_stream, username, overwrite=overwrite)
            copied_files = 1
            copied_bytes = src_status.length

            logger.info(f"Файл успешно скопирован: '{dst_file_path}' ({copied_bytes} байт)")
            return {
                "success": True,
                "message": f"Файл '{src_name}' успешно скопирован в кластер '{target_cluster.name}' ({dst_file_path})",
                "source_cluster_id": source_cluster.id,
                "source_path": clean_src,
                "target_cluster_id": target_cluster.id,
                "target_path": dst_file_path,
                "copied_files": copied_files,
                "copied_bytes": copied_bytes,
            }

        else:
            # Директория
            dest_dir_root = clean_dst
            if clean_dst == "/" or not clean_dst.endswith(f"/{src_name}"):
                dest_dir_root = f"{clean_dst.rstrip('/')}/{src_name}"

            await target_client.mkdirs(dest_dir_root, username)

            async def _copy_dir_recursive(curr_src: str, curr_dst: str):
                nonlocal copied_files, copied_bytes
                items = await source_client.list_status(curr_src, username)
                for item in items:
                    sub_src = f"{curr_src.rstrip('/')}/{item.pathSuffix}"
                    sub_dst = f"{curr_dst.rstrip('/')}/{item.pathSuffix}"

                    if item.type == "DIRECTORY":
                        await target_client.mkdirs(sub_dst, username)
                        await _copy_dir_recursive(sub_src, sub_dst)
                    else:
                        logger.info(
                            f"Потоковое копирование файла [{copied_files + 1}]: '{sub_src}' -> '{sub_dst}' ({item.length} байт)"
                        )
                        file_stream = source_client.open_stream(sub_src, username)
                        await target_client.create_file(sub_dst, file_stream, username, overwrite=overwrite)
                        copied_files += 1
                        copied_bytes += item.length

            await _copy_dir_recursive(clean_src, dest_dir_root)

            logger.info(
                f"Каталог успешно скопирован: '{dest_dir_root}', "
                f"всего файлов: {copied_files}, суммарный объем: {copied_bytes} байт"
            )
            return {
                "success": True,
                "message": f"Папка '{src_name}' успешно скопирована в кластер '{target_cluster.name}' ({dest_dir_root}), скопировано файлов: {copied_files}",
                "source_cluster_id": source_cluster.id,
                "source_path": clean_src,
                "target_cluster_id": target_cluster.id,
                "target_path": dest_dir_root,
                "copied_files": copied_files,
                "copied_bytes": copied_bytes,
            }


hdfs_service = HdfsService()
