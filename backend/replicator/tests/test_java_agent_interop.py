"""Тест межъязыковой интероперабельности (Python client -> Java Replicator Agent gRPC).

Проверяет:
1. Запуск скомпилированного Java Replicator Agent (Java 17).
2. Потоковую передачу файла из Python ReplicationWorkerClient в Java gRPC Receiver.
3. Точность расчета контрольной суммы SHA-256 и атомарный коммит на стороне Java.
"""

import hashlib
import os
import shutil
import socket
import subprocess
import tempfile
import time
import pytest

from backend.replicator.agent import ReplicationWorkerClient


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def get_java_cmd() -> str:
    if os.path.exists("/opt/homebrew/opt/openjdk/bin/java"):
        return "/opt/homebrew/opt/openjdk/bin/java"
    if os.environ.get("JAVA_HOME"):
        cand = os.path.join(os.environ["JAVA_HOME"], "bin", "java")
        if os.path.exists(cand):
            return cand
    return shutil.which("java") or "java"


@pytest.mark.asyncio
async def test_python_client_to_java_receiver_transfer():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    jar_path = os.path.join(
        repo_root,
        "backend/replicator/agent-java/target/replicator-agent-java-1.0.0-all.jar",
    )

    if not os.path.exists(jar_path):
        pytest.skip("JAR-файл Java Replicator Agent не найден (запустите mvn package)")

    java_cmd = get_java_cmd()
    try:
        ver = subprocess.run([java_cmd, "-version"], capture_output=True)
        if ver.returncode != 0:
            pytest.skip("Java недоступна в окружении")
    except Exception:
        pytest.skip("Java недоступна в окружении")

    staging_dir = tempfile.mkdtemp(prefix="java_agent_staging_")
    target_dir = tempfile.mkdtemp(prefix="java_agent_target_")
    port = find_free_port()

    # Запуск Java Replicator Agent в режиме receiver
    proc = subprocess.Popen(
        [
            java_cmd,
            "-jar",
            jar_path,
            "--port",
            str(port),
            "--mode",
            "receiver",
            "--staging-dir",
            staging_dir,
            "--orchestrator",
            "http://127.0.0.1:9999",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Ожидаем старта gRPC сервера
        time.sleep(2.0)

        # Создаем тестовый файл для отправки
        src_fd, src_path = tempfile.mkstemp(prefix="source_repl_")
        test_content = b"Hadoop Explorer: Cross-language replication from Python to Java Replicator Agent!\n" * 1024
        os.write(src_fd, test_content)
        os.close(src_fd)

        expected_sha256 = hashlib.sha256(test_content).hexdigest()
        target_path = os.path.join(target_dir, "committed_file.dat")

        client = ReplicationWorkerClient(
            orchestrator_url="http://127.0.0.1:9999",
            receiver_address=f"127.0.0.1:{port}",
            worker_id="python-test-sender",
        )

        response = await client.transfer_file(
            job_id="interop-job-001",
            source_path=src_path,
            target_path=target_path,
            run_as_service_account=True,
            target_address=f"127.0.0.1:{port}",
        )

        assert response.success is True
        assert response.bytes_written == len(test_content)
        assert response.checksum == expected_sha256
        assert os.path.exists(target_path)
        with open(target_path, "rb") as f:
            assert f.read() == test_content

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(staging_dir, ignore_errors=True)
        shutil.rmtree(target_dir, ignore_errors=True)
        if os.path.exists(src_path):
            os.remove(src_path)
