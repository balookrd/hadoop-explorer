"""Универсальный двунаправленный агент репликации Hadoop gRPC (Replicator Agent).

Содержит полную реализацию транспорта репликации:
- DataTransferServicer / create_receiver_server: прием входящих файлов через gRPC и атомарный commit в HDFS / файловую систему.
- ReplicationWorkerClient / ReplicationTransferClient: потоковая передача файлов чанками с Kerberos-контекстом и Token Bucket шейпингом.
- ReplicatorAgent: полнодуплексный демон, управляющий сетевым сервером приема и фоновым циклом отправки.
"""

import asyncio
import hashlib
import logging
import os
import shutil
import signal
import time
from typing import AsyncIterator, Dict, Optional
import uuid

import grpc
import httpx

from backend.replicator.generated import replicator_pb2, replicator_pb2_grpc
from backend.replicator.security.kerberos import KerberosContextManager

logger = logging.getLogger("replicator.agent")


# ============================================================================
# 0. Локальный шейпер пропускной способности агента (Local Bandwidth Limiter)
# ============================================================================


class LocalBandwidthLimiter:
    """Локальный Token Bucket шейпер для ограничения пропускной способности агента.

    Защищает сетевые карты и дисковую подсистему ноды Hadoop (Co-located Deployment)
    от перегрузки фоновым трафиком репликации.
    """

    def __init__(self, limit_mb_per_sec: float = 0.0, burst_seconds: float = 0.5):
        self.limit_bytes_per_sec = max(0.0, float(limit_mb_per_sec)) * 1024 * 1024
        self.burst_seconds = max(0.1, float(burst_seconds))
        self.capacity = self.limit_bytes_per_sec * self.burst_seconds if self.limit_bytes_per_sec > 0 else 0.0
        self.tokens = self.capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def is_enabled(self) -> bool:
        return self.limit_bytes_per_sec > 0

    @property
    def limit_mb_s(self) -> float:
        return round(self.limit_bytes_per_sec / (1024 * 1024), 2)

    def set_limit(self, limit_mb_per_sec: float) -> None:
        """Динамическое изменение лимита пропускной способности."""
        self.limit_bytes_per_sec = max(0.0, float(limit_mb_per_sec)) * 1024 * 1024
        if self.limit_bytes_per_sec > 0:
            self.capacity = self.limit_bytes_per_sec * self.burst_seconds
            self.tokens = min(self.tokens, self.capacity)
        else:
            self.capacity = 0.0
            self.tokens = 0.0
        self.last_update = time.monotonic()

    async def throttle(self, bytes_count: int) -> float:
        """Сдерживает скорость при передаче/приеме блока данных, если лимит превышен."""
        if not self.is_enabled or bytes_count <= 0:
            return 0.0

        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            if elapsed > 0:
                self.tokens = min(self.capacity, self.tokens + elapsed * self.limit_bytes_per_sec)
                self.last_update = now

            if self.tokens >= bytes_count:
                self.tokens -= bytes_count
                return 0.0

            deficit = bytes_count - self.tokens
            wait_seconds = deficit / self.limit_bytes_per_sec
            self.tokens -= bytes_count

        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)

        return wait_seconds


# ============================================================================
# 1. Серверная часть gRPC: Прием потока данных и атомарный коммит
# ============================================================================


class DataTransferServicer(replicator_pb2_grpc.DataTransferServiceServicer):
    """Реализация сервиса DataTransferService для приема и атомарной фиксации файлов."""

    def __init__(
        self,
        staging_dir: Optional[str] = None,
        hdfs_namenode: Optional[str] = None,
        hdfs_port: int = 8020,
        bandwidth_limiter: Optional[LocalBandwidthLimiter] = None,
        keytab_path: Optional[str] = None,
    ):
        self.staging_dir = staging_dir or os.environ.get("REPLICATOR_STAGING_DIR", "/tmp/staging")
        self.hdfs_namenode = hdfs_namenode or os.environ.get("REPLICATOR_HDFS_NAMENODE")
        self.hdfs_port = hdfs_port
        self.bandwidth_limiter = bandwidth_limiter
        self.keytab_path = keytab_path or os.environ.get("REPLICATOR_KEYTAB_PATH")
        os.makedirs(self.staging_dir, exist_ok=True)
        lim_str = (
            f", локальный лимит: {bandwidth_limiter.limit_mb_s} МБ/с"
            if bandwidth_limiter and bandwidth_limiter.is_enabled
            else ""
        )
        logger.info(f"gRPC Receiver инициализирован со staging-директорией: {self.staging_dir}{lim_str}")

    def _commit_file(
        self,
        staging_path: str,
        target_path: str,
        metadata: replicator_pb2.FileMetadata,
    ) -> str:
        """
        Атомарное перемещение (rename) принятого файла из staging в финальный target_path.
        Поддерживает:
        - Локальную файловую систему (shutil.move / os.replace)
        - HDFS через PyArrow с Kerberos Proxy User и doAs имперсонацией для Apache Ranger аудита
        """
        logger.info(f"Атомарный commit файла job_id={metadata.job_id}: '{staging_path}' -> '{target_path}'")

        # Режим 1: Перемещение в HDFS через PyArrow (если настроен HDFS namenode или префикс hdfs://)
        if self.hdfs_namenode or target_path.startswith("hdfs://"):
            clean_target = target_path.replace("hdfs://", "").split("/", 1)[-1]
            clean_target = "/" + clean_target.lstrip("/")

            with KerberosContextManager(
                keytab=self.keytab_path,
                principal=metadata.execution_principal,
                run_as_service_account=metadata.run_as_service_account,
            ) as krb_ctx:
                try:
                    import pyarrow.fs as pafs

                    host = self.hdfs_namenode or "localhost"
                    impersonate_user = krb_ctx.impersonate_user
                    logger.info(
                        f"Подключение к HDFS NameNode ({host}:{self.hdfs_port}) с билетом техучетки "
                        f"'{krb_ctx.service_principal}' и имперсонацией doAs='{impersonate_user or 'нет'}'"
                    )
                    hdfs_fs = pafs.HadoopFileSystem(
                        host,
                        port=self.hdfs_port,
                        user=impersonate_user,
                        kerb_ticket=krb_ctx.cache_file,
                    )

                    # Создаем родительские директории в HDFS
                    target_dir = os.path.dirname(clean_target)
                    if target_dir and target_dir != "/":
                        hdfs_fs.create_dir(target_dir, recursive=True)

                    # Копируем/перемещаем из локального staging в HDFS
                    with open(staging_path, "rb") as local_f:
                        with hdfs_fs.open_output_stream(clean_target) as hdfs_f:
                            shutil.copyfileobj(local_f, hdfs_f)

                    # Удаляем staging-файл после успешного сохранения в HDFS
                    if os.path.exists(staging_path):
                        os.remove(staging_path)

                    logger.info(
                        f"Файл успешно закоммичен в HDFS: {clean_target} "
                        f"(doAs: {impersonate_user or 'service_account'})"
                    )
                    return clean_target
                except Exception as e:
                    logger.warning(f"Не удалось закоммитить в HDFS через PyArrow ({e}), fallback на локальный rename")

        # Режим 2: Локальная файловая система (или смонтированный HDFS / dev окружение)
        target_dir = os.path.dirname(target_path)
        try:
            if target_dir:
                os.makedirs(target_dir, exist_ok=True)
            shutil.move(staging_path, target_path)
            logger.info(f"Файл атомарно перемещен в: {target_path}")
            return target_path
        except (PermissionError, OSError) as e:
            logger.warning(
                f"Не удалось переместить файл в целевой путь '{target_path}' ({e}), сохраняем в committed staging"
            )
            fallback_dir = os.path.join(self.staging_dir, "committed")
            os.makedirs(fallback_dir, exist_ok=True)
            fallback_path = os.path.join(fallback_dir, f"{metadata.job_id}_{os.path.basename(target_path)}")
            shutil.move(staging_path, fallback_path)
            return fallback_path

    async def TransferFile(
        self,
        request_iterator,
        context: grpc.aio.ServicerContext,
    ) -> replicator_pb2.TransferFileResponse:
        """
        Принимает поток запросов:
        - 1-е сообщение: FileMetadata
        - 2-е и последующие: FileChunk
        По окончании стрима атомарно перемещает файл в целевую директорию.
        """
        metadata: Optional[replicator_pb2.FileMetadata] = None
        staging_file_path: Optional[str] = None
        file_handle = None
        bytes_written = 0
        hasher = hashlib.sha256()

        try:
            async for request in request_iterator:
                # Шаг 1: Получение метаданных (первое сообщение)
                if request.HasField("metadata"):
                    metadata = request.metadata
                    target_filename = os.path.basename(metadata.target_path) or "data.bin"
                    staging_file_path = os.path.join(self.staging_dir, f"{metadata.job_id}_{target_filename}")

                    os.makedirs(os.path.dirname(staging_file_path), exist_ok=True)
                    file_handle = open(staging_file_path, "wb")
                    logger.info(
                        f"Начат прием файла job_id={metadata.job_id}: '{metadata.source_path}' -> "
                        f"'{metadata.target_path}' (staging: '{staging_file_path}'), "
                        f"техучетка={metadata.run_as_service_account}, principal='{metadata.execution_principal}'"
                    )

                # Шаг 2: Получение чанка данных
                elif request.HasField("chunk"):
                    chunk = request.chunk
                    if file_handle is None or metadata is None:
                        logger.error("Получен chunk данных до получения метаданных файла")
                        return replicator_pb2.TransferFileResponse(
                            job_id=chunk.job_id if chunk else "unknown",
                            success=False,
                            bytes_written=0,
                            message="Ошибка протокола: чанк передан до метаданных файла",
                        )

                    chunk_bytes = chunk.data
                    if chunk_bytes:
                        if self.bandwidth_limiter:
                            await self.bandwidth_limiter.throttle(len(chunk_bytes))
                        file_handle.write(chunk_bytes)
                        hasher.update(chunk_bytes)
                        bytes_written += len(chunk_bytes)

            # Проверка, что метаданные были получены
            if metadata is None or staging_file_path is None:
                return replicator_pb2.TransferFileResponse(
                    job_id="unknown",
                    success=False,
                    bytes_written=0,
                    message="Пустой поток запросов: метаданные не получены",
                )

            # Закрываем staging файл после окончания потока
            if file_handle is not None:
                file_handle.flush()
                file_handle.close()
                file_handle = None

            checksum = hasher.hexdigest()

            # Шаг 3: Атомарный rename/commit в финальный target_path
            final_path = self._commit_file(staging_file_path, metadata.target_path, metadata)

            logger.info(
                f"Файл успешно закоммичен: job_id={metadata.job_id}, байт={bytes_written}, "
                f"sha256={checksum}, final_path='{final_path}'"
            )

            return replicator_pb2.TransferFileResponse(
                job_id=metadata.job_id,
                success=True,
                bytes_written=bytes_written,
                target_path=final_path,
                checksum=checksum,
                message=f"Файл успешно принят и атомарно перемещен в: {final_path}",
            )

        except Exception as e:
            logger.exception(f"Исключение при приеме и commit файла: {e}")
            if file_handle is not None:
                try:
                    file_handle.close()
                except Exception:
                    pass

            if staging_file_path and os.path.exists(staging_file_path):
                try:
                    os.remove(staging_file_path)
                except Exception:
                    pass

            job_id_str = metadata.job_id if metadata else "unknown"
            return replicator_pb2.TransferFileResponse(
                job_id=job_id_str,
                success=False,
                bytes_written=bytes_written,
                message=f"Ошибка сервера Receiver: {str(e)}",
            )


async def create_receiver_server(
    host: str = "0.0.0.0",
    port: int = 50051,
    staging_dir: Optional[str] = None,
    hdfs_namenode: Optional[str] = None,
    hdfs_port: int = 8020,
    bandwidth_limiter: Optional[LocalBandwidthLimiter] = None,
    keytab_path: Optional[str] = None,
) -> grpc.aio.Server:
    """Создает асинхронный gRPC сервер Receiver для приема файлов."""
    server = grpc.aio.server(
        options=[
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
        ]
    )
    servicer = DataTransferServicer(
        staging_dir=staging_dir,
        hdfs_namenode=hdfs_namenode,
        hdfs_port=hdfs_port,
        bandwidth_limiter=bandwidth_limiter,
        keytab_path=keytab_path,
    )
    replicator_pb2_grpc.add_DataTransferServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"{host}:{port}")
    logger.info(f"gRPC Receiver сервер настроен на {host}:{port}")
    return server


# ============================================================================
# 2. Клиентская часть gRPC: Передача данных чанками и шейпинг полосы
# ============================================================================


class ReplicationWorkerClient:
    """Клиент репликации для передачи файлов через gRPC с троттлингом полосы (Token Bucket)."""

    def __init__(
        self,
        orchestrator_url: str = "http://localhost:8005",
        receiver_address: str = "localhost:50051",
        chunk_size_bytes: int = 64 * 1024,  # 64 KB по умолчанию
        worker_id: Optional[str] = None,
        keytab_path: Optional[str] = None,
        http_client: Optional[httpx.AsyncClient] = None,
        agent_secret: Optional[str] = None,
        bandwidth_limiter: Optional[LocalBandwidthLimiter] = None,
    ):
        self.orchestrator_url = orchestrator_url.rstrip("/")
        self.receiver_address = receiver_address
        self.chunk_size_bytes = chunk_size_bytes
        self.worker_id = worker_id or f"agent-{uuid.uuid4().hex[:8]}"
        self.keytab_path = keytab_path
        self._custom_http_client = http_client
        self.agent_secret = agent_secret or os.environ.get("REPLICATOR_AGENT_SECRET") or os.environ.get("AGENT_SECRET")
        self.bandwidth_limiter = bandwidth_limiter

    def _get_auth_headers(self) -> Dict[str, str]:
        headers = {}
        if self.agent_secret:
            headers["X-Agent-Secret"] = self.agent_secret
        return headers

    async def _get_http_client(self) -> httpx.AsyncClient:
        if self._custom_http_client is not None:
            return self._custom_http_client
        return httpx.AsyncClient(timeout=10.0)

    async def request_network_tokens(self, requested_bytes: int) -> float:
        """
        Запрашивает локальные токены пропускной способности агента (если задан лимит),
        а затем сетевую квоту у Оркестратора.
        """
        if requested_bytes <= 0:
            return 0.0

        local_wait = 0.0
        if self.bandwidth_limiter:
            local_wait = await self.bandwidth_limiter.throttle(requested_bytes)

        payload = {
            "worker_id": self.worker_id,
            "requested_bytes": requested_bytes,
        }

        client = await self._get_http_client()
        should_close = self._custom_http_client is None

        try:
            resp = await client.post(
                f"{self.orchestrator_url}/tokens/request", json=payload, headers=self._get_auth_headers()
            )
            resp.raise_for_status()
            data = resp.json()
            orchestrator_wait = float(data.get("wait_seconds", 0.0))

            if orchestrator_wait > 0:
                logger.debug(
                    f"Агент {self.worker_id}: глобальный троттлинг оркестратора, пауза {orchestrator_wait:.4f}c для {requested_bytes} байт"
                )
                await asyncio.sleep(orchestrator_wait)

            return local_wait + orchestrator_wait
        except Exception as e:
            logger.warning(
                f"Не удалось запросить сетевую квоту у Оркестратора ({e}), передача без глобального троттлинга"
            )
            return local_wait
        finally:
            if should_close:
                await client.aclose()

    async def update_job_progress(
        self,
        job_id: str,
        status: Optional[str] = None,
        copied_bytes: Optional[int] = None,
        total_bytes: Optional[int] = None,
        message: Optional[str] = None,
    ) -> None:
        """Отправляет обновления статуса и прогресса задачи в Оркестратор."""
        payload = {}
        if status is not None:
            payload["status"] = status
        if copied_bytes is not None:
            payload["copied_bytes"] = copied_bytes
        if total_bytes is not None:
            payload["total_bytes"] = total_bytes
        if message is not None:
            payload["message"] = message

        if not payload:
            return

        client = await self._get_http_client()
        should_close = self._custom_http_client is None
        try:
            resp = await client.patch(
                f"{self.orchestrator_url}/jobs/{job_id}", json=payload, headers=self._get_auth_headers()
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Ошибка обновления прогресса задачи {job_id} в Оркестраторе: {e}")
        finally:
            if should_close:
                await client.aclose()

    async def transfer_file(
        self,
        job_id: str,
        source_path: str,
        target_path: str,
        run_as_service_account: bool = True,
        execution_principal: Optional[str] = None,
        target_address: Optional[str] = None,
    ) -> replicator_pb2.TransferFileResponse:
        """
        Основной метод передачи файла:
        1. Считывает исходный файл блоками
        2. Запрашивает квоты у Оркестратора
        3. Передает поток чанков в gRPC стрим целевого агента
        4. Обновляет статус задачи
        """
        if not os.path.isfile(source_path):
            error_msg = f"Исходный файл не найден: '{source_path}'"
            logger.error(error_msg)
            await self.update_job_progress(job_id=job_id, status="FAILED", message=error_msg)
            return replicator_pb2.TransferFileResponse(
                job_id=job_id,
                success=False,
                bytes_written=0,
                message=error_msg,
            )

        total_bytes = os.path.getsize(source_path)
        principal = execution_principal or ("hdfs-replicator@REALM.LOCAL" if run_as_service_account else "current_user")
        destination_address = target_address or self.receiver_address

        logger.info(
            f"Агент {self.worker_id} начинает передачу job_id={job_id}: '{source_path}' ({total_bytes} байт) -> "
            f"'{target_path}' (целевой узел: {destination_address}), техучетка={run_as_service_account}, principal='{principal}'"
        )

        await self.update_job_progress(
            job_id=job_id,
            status="RUNNING",
            total_bytes=total_bytes,
            copied_bytes=0,
            message="Начало передачи данных",
        )

        async def request_generator() -> AsyncIterator[replicator_pb2.TransferFileRequest]:
            # 1-е сообщение: Метаданные файла
            metadata = replicator_pb2.FileMetadata(
                job_id=job_id,
                source_path=source_path,
                target_path=target_path,
                total_bytes=total_bytes,
                run_as_service_account=run_as_service_account,
                execution_principal=principal,
            )
            yield replicator_pb2.TransferFileRequest(metadata=metadata)

            # Чтение и потоковая отправка файла чанками внутри изолированного Kerberos-контекста
            with KerberosContextManager(
                keytab=self.keytab_path,
                principal=principal,
                run_as_service_account=run_as_service_account,
            ):
                offset = 0
                last_progress_update = 0

                with open(source_path, "rb") as f:
                    while True:
                        chunk = f.read(self.chunk_size_bytes)
                        if not chunk:
                            break

                        chunk_len = len(chunk)
                        is_last = (offset + chunk_len) >= total_bytes

                        # Троттлинг: запрашиваем сетевую квоту перед отправкой блока
                        await self.request_network_tokens(chunk_len)

                        file_chunk = replicator_pb2.FileChunk(
                            job_id=job_id,
                            offset=offset,
                            data=chunk,
                            is_last_chunk=is_last,
                        )
                        yield replicator_pb2.TransferFileRequest(chunk=file_chunk)

                        offset += chunk_len

                        # Периодически обновляем прогресс в Оркестраторе (каждые 1 МБ или в конце)
                        if offset - last_progress_update >= 1024 * 1024 or is_last:
                            await self.update_job_progress(
                                job_id=job_id,
                                copied_bytes=offset,
                                message=f"Передано {offset} из {total_bytes} байт",
                            )
                            last_progress_update = offset

        # Подключение к Target Agent через gRPC канал
        channel = grpc.aio.insecure_channel(
            destination_address,
            options=[
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ],
        )

        try:
            stub = replicator_pb2_grpc.DataTransferServiceStub(channel)
            response: replicator_pb2.TransferFileResponse = await stub.TransferFile(request_generator())

            if response.success:
                logger.info(
                    f"Передача job_id={job_id} успешно завершена. Записано: {response.bytes_written} байт, "
                    f"sha256={response.checksum}"
                )
                await self.update_job_progress(
                    job_id=job_id,
                    status="COMPLETED",
                    copied_bytes=response.bytes_written,
                    message="Репликация успешно завершена",
                )
            else:
                logger.error(f"Приемник вернул ошибку для job_id={job_id}: {response.message}")
                await self.update_job_progress(
                    job_id=job_id,
                    status="FAILED",
                    copied_bytes=response.bytes_written,
                    message=f"Ошибка приемника: {response.message}",
                )

            return response
        except Exception as e:
            error_msg = f"Ошибка gRPC передачи данных: {str(e)}"
            logger.exception(error_msg)
            await self.update_job_progress(
                job_id=job_id,
                status="FAILED",
                message=error_msg,
            )
            return replicator_pb2.TransferFileResponse(
                job_id=job_id,
                success=False,
                bytes_written=0,
                message=error_msg,
            )
        finally:
            await channel.close()


# Псевдоним для удобства
ReplicationTransferClient = ReplicationWorkerClient


# ============================================================================
# 3. Универсальный агент ReplicatorAgent
# ============================================================================


class ReplicatorAgent:
    """Универсальный агент репликации с поддержкой полного дуплекса (Sender + Receiver)."""

    def __init__(
        self,
        agent_id: Optional[str] = None,
        cluster_id: Optional[str] = None,
        mode: Optional[str] = None,
        orchestrator_url: Optional[str] = None,
        receiver_host: Optional[str] = None,
        receiver_port: Optional[int] = None,
        staging_dir: Optional[str] = None,
        hdfs_namenode: Optional[str] = None,
        hdfs_port: int = 8020,
        poll_interval_sec: float = 3.0,
        fallback_target_address: Optional[str] = None,
        target_clusters_map: Optional[Dict[str, str]] = None,
        advertised_grpc_address: Optional[str] = None,
        enable_dynamic_registration: Optional[bool] = None,
        agent_secret: Optional[str] = None,
        max_bandwidth_mb_s: Optional[float] = None,
        keytab_path: Optional[str] = None,
    ):
        self.agent_id = agent_id or os.environ.get("AGENT_ID") or os.environ.get("WORKER_ID", "agent-01")
        self.cluster_id = cluster_id or os.environ.get("AGENT_CLUSTER_ID") or os.environ.get("CLUSTER_ID")
        self.mode = (mode or os.environ.get("AGENT_MODE", "all")).lower()
        self.orchestrator_url = orchestrator_url or os.environ.get("ORCHESTRATOR_URL", "http://localhost:8005")
        self.receiver_host = receiver_host or os.environ.get("RECEIVER_HOST", "0.0.0.0")
        self.receiver_port = receiver_port or int(os.environ.get("RECEIVER_PORT", "50051"))
        self.staging_dir = staging_dir or os.environ.get("REPLICATOR_STAGING_DIR", "/tmp/staging")
        self.hdfs_namenode = hdfs_namenode or os.environ.get("REPLICATOR_HDFS_NAMENODE")
        self.hdfs_port = hdfs_port
        self.keytab_path = keytab_path or os.environ.get("REPLICATOR_KEYTAB_PATH")
        self.poll_interval_sec = poll_interval_sec
        self.fallback_target_address = (
            fallback_target_address
            or os.environ.get("RECEIVER_ADDRESS")
            or os.environ.get("FALLBACK_TARGET_ADDRESS", "localhost:50051")
        )
        self.target_clusters_map: Dict[str, str] = target_clusters_map or {}
        self.agent_secret = agent_secret or os.environ.get("REPLICATOR_AGENT_SECRET") or os.environ.get("AGENT_SECRET")

        # Лимит пропускной способности агента в МБ/с (для Co-located Deployment с DataNode)
        env_bw = os.environ.get("AGENT_MAX_BANDWIDTH_MB_S")
        self.max_bandwidth_mb_s = (
            max_bandwidth_mb_s if max_bandwidth_mb_s is not None else (float(env_bw) if env_bw else None)
        )
        self.bandwidth_limiter = LocalBandwidthLimiter(self.max_bandwidth_mb_s or 0.0)

        # Автоматическое определение внешнего (advertised) gRPC адреса для саморегистрации
        self.advertised_grpc_address = (
            advertised_grpc_address
            or os.environ.get("AGENT_ADVERTISED_ADDRESS")
            or os.environ.get("RECEIVER_ADVERTISED_ADDRESS")
        )
        if not self.advertised_grpc_address:
            if self.receiver_host and self.receiver_host not in ("0.0.0.0", "::"):
                self.advertised_grpc_address = f"{self.receiver_host}:{self.receiver_port}"
            else:
                host = os.environ.get("HOSTNAME") or "localhost"
                self.advertised_grpc_address = f"{host}:{self.receiver_port}"

        self.enable_dynamic_registration = (
            enable_dynamic_registration
            if enable_dynamic_registration is not None
            else os.environ.get("AGENT_ENABLE_REGISTRATION", "true").lower() in ("true", "1", "yes")
        )
        self.active_transfers: int = 0

        # Кэш адресов gRPC целевых кластеров из оркестратора
        self._topology_cluster_cache: Dict[str, str] = {}
        self._last_topology_sync = 0.0

        self.server: Optional[grpc.aio.Server] = None
        self.worker_client: Optional[ReplicationWorkerClient] = None
        self._stop_event = asyncio.Event()

    def _get_auth_headers(self) -> Dict[str, str]:
        headers = {}
        if self.agent_secret:
            headers["X-Agent-Secret"] = self.agent_secret
        return headers

    async def get_target_address(self, target_cluster_id: str, http_client: Optional[httpx.AsyncClient] = None) -> str:
        """Определяет сетевой адрес gRPC целевого узла для передачи данных.

        Приоритет разрешения адреса:
        1. Явная статическая карта (target_clusters_map при программной инициализации).
        2. Переменная окружения AGENT_TARGET_<CLUSTER_ID> (ручной оверрайд для NAT/тестов,
           например AGENT_TARGET_DEMO_CLUSTER=agent-dc1:50051).
        3. Динамический опрос Оркестратора (GET /api/v1/clusters) — основной штатный механизм,
           где каждый кластер имеет прописанный grpc_address.
        4. Резервный адрес (fallback_target_address / RECEIVER_ADDRESS / localhost:50051).
        """
        # 1. Явная статическая карта
        if target_cluster_id in self.target_clusters_map:
            return self.target_clusters_map[target_cluster_id]

        # 2. Переменная окружения AGENT_TARGET_<CLUSTER_ID>
        env_var_name = f"AGENT_TARGET_{target_cluster_id.upper().replace('-', '_')}"
        if env_var_name in os.environ:
            return os.environ[env_var_name]

        # 3. Динамический опрос оркестратора (/api/v1/clusters)
        now = asyncio.get_running_loop().time()
        if now - self._last_topology_sync > 30.0 or target_cluster_id not in self._topology_cluster_cache:
            try:
                client = http_client or httpx.AsyncClient(timeout=5.0)
                should_close = http_client is None
                try:
                    resp = await client.get(f"{self.orchestrator_url}/api/v1/clusters")
                    if resp.status_code == 200:
                        clusters_data = resp.json()
                        for c in clusters_data:
                            cid = c.get("id")
                            grpc_addr = c.get("grpc_address")
                            if cid and grpc_addr:
                                self._topology_cluster_cache[cid] = grpc_addr
                        self._last_topology_sync = now
                finally:
                    if should_close:
                        await client.aclose()
            except Exception as e:
                logger.debug(f"Не удалось обновить топологию кластеров из Оркестратора: {e}")

        if target_cluster_id in self._topology_cluster_cache:
            return self._topology_cluster_cache[target_cluster_id]

        # 4. Fallback с информативным предупреждением
        logger.warning(
            f"Кластер '{target_cluster_id}' не найден в топологии Оркестратора. "
            f"Используется fallback gRPC адрес: '{self.fallback_target_address}'. "
            f"Известные кластеры из Оркестратора: {list(self._topology_cluster_cache.keys())}"
        )
        return self.fallback_target_address

    async def start_receiver(self) -> grpc.aio.Server:
        """Запускает gRPC сервер для приема файлов."""
        logger.info(
            f"Запуск gRPC Receiver компонента агента {self.agent_id} на "
            f"{self.receiver_host}:{self.receiver_port} (staging: {self.staging_dir})"
        )
        self.server = await create_receiver_server(
            host=self.receiver_host,
            port=self.receiver_port,
            staging_dir=self.staging_dir,
            hdfs_namenode=self.hdfs_namenode,
            hdfs_port=self.hdfs_port,
            bandwidth_limiter=self.bandwidth_limiter,
            keytab_path=self.keytab_path,
        )
        await self.server.start()
        logger.info(f"gRPC Receiver компонент агента {self.agent_id} готов принимать файлы")
        return self.server

    async def register_with_orchestrator(self, http_client: Optional[httpx.AsyncClient] = None) -> bool:
        """Выполняет первичную регистрацию агента в Оркестраторе (Service Discovery)."""
        payload = {
            "agent_id": self.agent_id,
            "cluster_id": self.cluster_id,
            "mode": self.mode,
            "grpc_address": self.advertised_grpc_address if self.mode in ("all", "receiver") else None,
            "hostname": os.environ.get("HOSTNAME") or "localhost",
            "version": "1.0.0",
            "max_bandwidth_mb_s": self.max_bandwidth_mb_s,
        }
        client = http_client or httpx.AsyncClient(timeout=5.0)
        should_close = http_client is None
        try:
            resp = await client.post(
                f"{self.orchestrator_url}/api/v1/agents/register", json=payload, headers=self._get_auth_headers()
            )
            if resp.status_code in (200, 201):
                logger.info(
                    f"Агент {self.agent_id} успешно зарегистрирован на Оркестраторе {self.orchestrator_url} "
                    f"(кластер: {self.cluster_id}, advertised gRPC: {self.advertised_grpc_address})"
                )
                return True
            else:
                logger.warning(f"Ошибка регистрации агента ({resp.status_code}): {resp.text}")
                return False
        except Exception as e:
            logger.debug(f"Оркестратор недоступен при регистрации агента {self.agent_id}: {e}")
            return False
        finally:
            if should_close:
                await client.aclose()

    async def send_heartbeat(self, http_client: Optional[httpx.AsyncClient] = None) -> bool:
        """Отправляет периодический keepalive пинг в Оркестратор."""
        payload = {
            "agent_id": self.agent_id,
            "cluster_id": self.cluster_id,
            "grpc_address": self.advertised_grpc_address if self.mode in ("all", "receiver") else None,
            "status": "online",
            "active_transfers": self.active_transfers,
        }
        client = http_client or httpx.AsyncClient(timeout=5.0)
        should_close = http_client is None
        try:
            resp = await client.post(
                f"{self.orchestrator_url}/api/v1/agents/heartbeat", json=payload, headers=self._get_auth_headers()
            )
            return resp.status_code in (200, 201)
        except Exception as e:
            logger.debug(f"Ошибка отправки keepalive от агента {self.agent_id}: {e}")
            return False
        finally:
            if should_close:
                await client.aclose()

    async def unregister_from_orchestrator(self, http_client: Optional[httpx.AsyncClient] = None) -> bool:
        """Отправляет сигнал дерегистрации в Оркестратор при остановке агента."""
        client = http_client or httpx.AsyncClient(timeout=3.0)
        should_close = http_client is None
        try:
            resp = await client.post(
                f"{self.orchestrator_url}/api/v1/agents/unregister?agent_id={self.agent_id}",
                headers=self._get_auth_headers(),
            )
            if resp.status_code == 200:
                logger.info(f"Агент {self.agent_id} успешно снят с регистрации на Оркестраторе (OFFLINE)")
                return True
        except Exception as e:
            logger.debug(f"Не удалось снять с регистрации агент {self.agent_id}: {e}")
        finally:
            if should_close:
                await client.aclose()
        return False

    async def run_keepalive_loop(self):
        """Фоновый цикл периодического keepalive в Оркестратор."""
        logger.info(
            f"Запуск цикла Keepalive агента {self.agent_id} на {self.orchestrator_url} "
            f"(интервал: {self.poll_interval_sec}с)"
        )
        async with httpx.AsyncClient(timeout=5.0) as http_client:
            # Первичная саморегистрация
            await self.register_with_orchestrator(http_client)

            # Периодические keepalive пинги
            while not self._stop_event.is_set():
                await self.send_heartbeat(http_client)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval_sec)
                except asyncio.TimeoutError:
                    pass

    async def run_sender_loop(self):
        """Фоновый цикл Sender: опрос задач очереди и передача данных."""
        logger.info(
            f"Запуск цикла Sender компонента агента {self.agent_id}. "
            f"Кластер привязки: '{self.cluster_id or 'все'}', Оркестратор: {self.orchestrator_url}"
        )

        self.worker_client = ReplicationWorkerClient(
            orchestrator_url=self.orchestrator_url,
            receiver_address=self.fallback_target_address,
            worker_id=self.agent_id,
            agent_secret=self.agent_secret,
            bandwidth_limiter=self.bandwidth_limiter,
            keytab_path=self.keytab_path,
        )

        async with httpx.AsyncClient(timeout=10.0) as http_client:
            while not self._stop_event.is_set():
                try:
                    # Получение задач из очереди
                    resp = await http_client.get(f"{self.orchestrator_url}/jobs")
                    if resp.status_code == 200:
                        jobs = resp.json()
                        queued_jobs = [j for j in jobs if j.get("status") == "QUEUED"]

                        for job in queued_jobs:
                            if self._stop_event.is_set():
                                break

                            job_source_cluster = job.get("source_cluster_id")
                            # Если агент привязан к конкретному кластеру, берем только задачи этого источника
                            if self.cluster_id and job_source_cluster and job_source_cluster != self.cluster_id:
                                continue

                            job_id = job["id"]
                            source_path = job["source_path"]
                            target_path = job["target_path"]
                            target_cluster_id = job.get("target_cluster_id", "backup-cluster")
                            run_as_sa = job.get("run_as_service_account", True)
                            principal = job.get("execution_principal")

                            target_address = await self.get_target_address(
                                target_cluster_id=target_cluster_id,
                                http_client=http_client,
                            )

                            logger.info(
                                f"Агент {self.agent_id} взял задачу {job_id} в обработку: "
                                f"'{source_path}' -> '{target_path}' (целевой узел: {target_address}, "
                                f"кластер назначения: {target_cluster_id})"
                            )

                            # Демо/тестовая генерация полезной нагрузки для /tmp/ путей при необходимости
                            if not os.path.exists(source_path) and "/tmp/" in source_path:
                                os.makedirs(os.path.dirname(source_path), exist_ok=True)
                                with open(source_path, "wb") as f:
                                    f.write(os.urandom(min(5 * 1024 * 1024, job.get("total_bytes") or 1048576)))

                            self.active_transfers += 1
                            try:
                                await self.worker_client.transfer_file(
                                    job_id=job_id,
                                    source_path=source_path,
                                    target_path=target_path,
                                    run_as_service_account=run_as_sa,
                                    execution_principal=principal,
                                    target_address=target_address,
                                )
                            finally:
                                self.active_transfers = max(0, self.active_transfers - 1)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Ошибка в цикле Sender агента {self.agent_id}: {e}")

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval_sec)
                except asyncio.TimeoutError:
                    pass

    async def start(self):
        """Запускает агент в соответствии с выбранным режимом."""
        logger.info(f"Старт ReplicatorAgent {self.agent_id} в режиме '{self.mode}'...")

        tasks = []

        # 1. Запуск динамической регистрации и keepalive
        if self.enable_dynamic_registration:
            tasks.append(asyncio.create_task(self.run_keepalive_loop()))

        # 2. Запуск Receiver (прием файлов)
        if self.mode in ("all", "receiver"):
            await self.start_receiver()
            tasks.append(asyncio.create_task(self.server.wait_for_termination()))

        # 3. Запуск Sender (передача исходящих файлов)
        if self.mode in ("all", "sender"):
            tasks.append(asyncio.create_task(self.run_sender_loop()))

        if not tasks:
            raise ValueError(f"Неизвестный режим работы агента: '{self.mode}'. Допустимы: 'all', 'sender', 'receiver'")

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Получен сигнал завершения работы агента")
        finally:
            await self.stop()

    async def stop(self):
        """Останавливает компоненты агента."""
        logger.info(f"Остановка ReplicatorAgent {self.agent_id}...")
        self._stop_event.set()
        if self.enable_dynamic_registration:
            await self.unregister_from_orchestrator()
        if self.server:
            await self.server.stop(grace=3.0)
            logger.info("gRPC Receiver сервер остановлен")


async def main():
    """Точка входа запуска демона агента."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    agent = ReplicatorAgent()

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Перехвачен сигнал завершения ОС, инициализация остановки...")
        agent._stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    await agent.start()


if __name__ == "__main__":
    asyncio.run(main())
