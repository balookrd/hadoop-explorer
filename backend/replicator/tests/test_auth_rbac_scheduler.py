"""Тесты для аутентификации, RBAC разграничения задач, шедулера и эндпоинтов топологии."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.replicator.orchestrator.db import get_db
from backend.replicator.orchestrator.main import app
from backend.replicator.orchestrator.models import Base, JobModel

TEST_DB_URL = "sqlite:///:memory:"
test_engine = create_engine(
    TEST_DB_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


@pytest.fixture(autouse=True)
def setup_test_db():
    Base.metadata.create_all(bind=test_engine)
    yield
    Base.metadata.drop_all(bind=test_engine)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def client():
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_auth_login_and_me(client):
    """Проверка входа под admin_user и analyst_user."""
    # Логин админа
    resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["user"]["is_admin"] is True
    assert data["user"]["system_role"] == "admin"
    admin_token = data["access_token"]

    # Проверка /me для админа
    me_resp = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {admin_token}"})
    assert me_resp.status_code == 200
    assert me_resp.json()["username"] == "admin_user"
    assert me_resp.json()["is_admin"] is True

    # Логин аналитика
    resp2 = client.post("/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"})
    assert resp2.status_code == 200
    analyst_token = resp2.json()["access_token"]
    assert resp2.json()["user"]["is_admin"] is False
    assert resp2.json()["user"]["system_role"] == "reader"

    # Проверка /me для аналитика
    me2 = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {analyst_token}"})
    assert me2.status_code == 200
    assert me2.json()["username"] == "analyst_user"
    assert me2.json()["is_admin"] is False


def test_rbac_admin_sees_all_jobs_user_sees_only_own(client):
    """
    Требование RBAC:
    Администратор должен видеть ВСЕ задачи всех пользователей.
    Обычный пользователь (analyst_user) видит только свои задачи.
    """
    # 1. Авторизуемся под admin_user
    admin_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    admin_token = admin_resp.json()["access_token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    # 2. Авторизуемся под analyst_user
    user_resp = client.post("/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"})
    user_token = user_resp.json()["access_token"]
    user_headers = {"Authorization": f"Bearer {user_token}"}

    # 3. Создаем задачу от admin_user
    job_admin = client.post(
        "/jobs",
        json={
            "source_path": "/data/admin_source",
            "target_path": "/backup/admin_target",
            "run_as_service_account": False,
        },
        headers=admin_headers,
    ).json()
    assert job_admin["created_by"] == "admin_user"

    # 4. Создаем задачу от analyst_user
    job_user = client.post(
        "/jobs",
        json={
            "source_path": "/data/user_source",
            "target_path": "/backup/user_target",
            "run_as_service_account": False,
        },
        headers=user_headers,
    ).json()
    assert job_user["created_by"] == "analyst_user"

    # 5. Проверяем /jobs как admin_user: ДОЛЖЕН видеть обе задачи!
    admin_list = client.get("/jobs", headers=admin_headers).json()
    job_ids_admin_sees = [j["id"] for j in admin_list]
    assert job_admin["id"] in job_ids_admin_sees
    assert job_user["id"] in job_ids_admin_sees
    assert len(admin_list) >= 2

    # 6. Проверяем /jobs как analyst_user: ДОЛЖЕН видеть ТОЛЬКО свою задачу!
    user_list = client.get("/jobs", headers=user_headers).json()
    job_ids_user_sees = [j["id"] for j in user_list]
    assert job_user["id"] in job_ids_user_sees
    assert job_admin["id"] not in job_ids_user_sees


def test_list_jobs_status_and_author_filters(client):
    """Проверка фильтрации задач репликации по статусу (status) и автору (author)."""
    admin_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    admin_token = admin_resp.json()["access_token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    user_resp = client.post("/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"})
    user_token = user_resp.json()["access_token"]
    user_headers = {"Authorization": f"Bearer {user_token}"}

    # Создаем задачу от admin_user
    job_admin = client.post(
        "/jobs",
        json={"source_path": "/data/filter_adm", "target_path": "/backup/filter_adm"},
        headers=admin_headers,
    ).json()

    # Создаем задачу от analyst_user
    job_user = client.post(
        "/jobs",
        json={"source_path": "/data/filter_usr", "target_path": "/backup/filter_usr"},
        headers=user_headers,
    ).json()

    # 1. Фильтр по автору: author=analyst_user
    res = client.get("/jobs?author=analyst_user", headers=admin_headers).json()
    assert all(j["created_by"] == "analyst_user" for j in res)
    assert any(j["id"] == job_user["id"] for j in res)
    assert all(j["id"] != job_admin["id"] for j in res)

    # 2. Фильтр по автору: author=admin_user
    res = client.get("/jobs?author=admin_user", headers=admin_headers).json()
    assert all(j["created_by"] == "admin_user" for j in res)
    assert any(j["id"] == job_admin["id"] for j in res)
    assert all(j["id"] != job_user["id"] for j in res)

    # 3. Фильтр по статусу: status=QUEUED
    res = client.get("/jobs?status=QUEUED", headers=admin_headers).json()
    assert all(j["status"] == "QUEUED" for j in res)
    assert any(j["id"] == job_admin["id"] for j in res)
    assert any(j["id"] == job_user["id"] for j in res)

    # 4. Фильтр по несуществующему статусу/комбинации
    res = client.get("/jobs?status=CANCELLED&author=analyst_user", headers=admin_headers).json()
    assert len(res) == 0

    # 5. Обычный пользователь не может увидеть чужие задачи даже с author=admin_user
    res = client.get("/jobs?author=admin_user", headers=user_headers).json()
    assert len(res) == 0


def test_scheduler_job_creation(client):
    """Проверка создания запланированной задачи с расчетом next_run_at."""
    admin_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    token = admin_resp.json()["access_token"]

    resp = client.post(
        "/jobs",
        json={
            "source_path": "/data/events",
            "target_path": "/backup/events",
            "is_scheduled": True,
            "cron_expression": "@every_5m",
            "run_as_service_account": True,
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 201
    job = resp.json()
    assert job["is_scheduled"] is True
    assert job["status"] == "SCHEDULED"
    assert job["cron_expression"] == "@every_5m"
    assert job["next_run_at"] is not None


def test_runs_history_retention_and_scheduled_status(client):
    """Проверка истории запусков (JobRun), настройки глубины истории и статуса SCHEDULED."""
    admin_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    token = admin_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 1. Создаем ручную задачу с глубиной истории = 3
    create_resp = client.post(
        "/jobs",
        json={
            "source_path": "/data/history_test",
            "target_path": "/backup/history_test",
            "total_bytes": 10485760,
            "is_scheduled": False,
            "history_retention_runs": 3,
        },
        headers=headers,
    )
    assert create_resp.status_code == 201
    job = create_resp.json()
    job_id = job["id"]
    assert job["status"] == "QUEUED"
    assert job["history_retention_runs"] == 3

    # 2. Проверяем наличие первого запуска
    runs_resp = client.get(f"/jobs/{job_id}/runs", headers=headers)
    assert runs_resp.status_code == 200
    runs = runs_resp.json()
    assert len(runs) == 1
    assert runs[0]["run_number"] == 1
    assert runs[0]["trigger_type"] == "MANUAL"
    assert runs[0]["status"] == "QUEUED"

    # 3. Воркер переводит в RUNNING, затем в COMPLETED
    client.patch(f"/jobs/{job_id}", json={"status": "RUNNING", "copied_bytes": 1048576})
    client.patch(f"/jobs/{job_id}", json={"status": "COMPLETED", "copied_bytes": 10485760})

    # Проверяем, что запуск #1 завершен со статистикой
    runs = client.get(f"/jobs/{job_id}/runs", headers=headers).json()
    assert len(runs) == 1
    assert runs[0]["run_number"] == 1
    assert runs[0]["status"] == "COMPLETED"
    assert runs[0]["copied_bytes"] == 10485760

    # 4. Перезапускаем задачу вручную -> запуск #2
    start_resp = client.post(f"/jobs/{job_id}/start", headers=headers)
    assert start_resp.status_code == 200
    runs = client.get(f"/jobs/{job_id}/runs", headers=headers).json()
    assert len(runs) == 2
    assert runs[0]["run_number"] == 2
    assert runs[0]["status"] == "QUEUED"

    # Завершаем запуск #2
    client.patch(f"/jobs/{job_id}", json={"status": "COMPLETED", "copied_bytes": 10485760})

    # 5. Запускаем #3
    client.post(f"/jobs/{job_id}/start", headers=headers)
    client.patch(f"/jobs/{job_id}", json={"status": "COMPLETED", "copied_bytes": 10485760})
    runs = client.get(f"/jobs/{job_id}/runs", headers=headers).json()
    assert len(runs) == 3

    # 6. Запускаем #4 -> с учетом retention=3, старейший запуск #1 должен быть удален, остаются [#4, #3, #2]
    client.post(f"/jobs/{job_id}/start", headers=headers)
    runs = client.get(f"/jobs/{job_id}/runs", headers=headers).json()
    assert len(runs) == 3
    run_numbers = [r["run_number"] for r in runs]
    assert run_numbers == [4, 3, 2]

    # 7. Проверка периодической задачи (scheduled)
    sched_resp = client.post(
        "/jobs",
        json={
            "source_path": "/data/sched_cycle",
            "target_path": "/backup/sched_cycle",
            "is_scheduled": True,
            "cron_expression": "@every_5m",
            "total_bytes": 5000000,
        },
        headers=headers,
    )
    sched_job = sched_resp.json()
    sched_id = sched_job["id"]
    # До начала исполнения периодическая задача имеет статус SCHEDULED
    assert sched_job["status"] == "SCHEDULED"

    # Запускаем шедулед задачу через start
    client.post(f"/jobs/{sched_id}/start", headers=headers)
    job_running = client.get(f"/jobs/{sched_id}", headers=headers).json()
    assert job_running["status"] == "QUEUED"

    # Воркер выполняет её и рапортует COMPLETED
    client.patch(f"/jobs/{sched_id}", json={"status": "RUNNING", "copied_bytes": 1000})
    client.patch(f"/jobs/{sched_id}", json={"status": "COMPLETED", "copied_bytes": 5000000})

    # Периодическая задача после завершения итерации возвращается в SCHEDULED!
    job_after = client.get(f"/jobs/{sched_id}", headers=headers).json()
    assert job_after["status"] == "SCHEDULED"
    assert "завершен" in job_after["message"].lower()

    # А в истории запусков зафиксирован выполненный запуск COMPLETED!
    sched_runs = client.get(f"/jobs/{sched_id}/runs", headers=headers).json()
    assert len(sched_runs) == 1
    assert sched_runs[0]["status"] == "COMPLETED"


def test_topology_and_limits_endpoints(client):
    """Проверка эндпоинтов топологии ЦОД, кластеров и управления полосой."""
    # Получение кластеров
    cl_resp = client.get("/api/v1/clusters")
    assert cl_resp.status_code == 200
    clusters = cl_resp.json()
    assert len(clusters) >= 2
    assert any(c["id"] == "demo-cluster" and c["dc_id"] == "dc1" for c in clusters)
    assert any(c["id"] == "backup-cluster" and c["dc_id"] == "dc2" for c in clusters)

    # Получение топологии
    top_resp = client.get("/api/v1/topology")
    assert top_resp.status_code == 200
    top = top_resp.json()
    assert len(top["datacenters"]) >= 2
    assert len(top["dc_limits"]) >= 1

    # Установка лимита DC-DC
    dc_lim_resp = client.post(
        "/api/v1/limits/dc-dc",
        json={"source_dc": "dc1", "target_dc": "dc2", "limit_mb_per_sec": 75.0},
    )
    assert dc_lim_resp.status_code == 200

    # Установка лимита HDFS-HDFS
    hdfs_lim_resp = client.post(
        "/api/v1/limits/hdfs-hdfs",
        json={"source_cluster": "demo-cluster", "target_cluster": "backup-cluster", "limit_mb_per_sec": 35.0},
    )
    assert hdfs_lim_resp.status_code == 200


def test_topology_limits_rbac_forbidden_for_non_admin(client):
    """
    Проверка RBAC:
    Пользователь без прав администратора (analyst_user) получает 403 Forbidden
    при попытке изменить любые лимиты топологии (Global, DC-DC, HDFS-HDFS, Token Bucket).
    Администратор (admin_user) успешно изменяет лимиты (HTTP 200).
    """
    # 1. Авторизуемся под обычным пользователем analyst_user
    user_resp = client.post("/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"})
    assert user_resp.status_code == 200
    user_token = user_resp.json()["access_token"]
    user_headers = {"Authorization": f"Bearer {user_token}"}

    # 2. Попытка изменить глобальный лимит -> 403
    resp_global = client.post(
        "/api/v1/limits/global",
        json={"limit_bytes_per_sec": 50 * 1024 * 1024},
        headers=user_headers,
    )
    assert resp_global.status_code == 403
    assert "Только администратор" in resp_global.json()["detail"]

    # 3. Попытка изменить лимит DC-DC -> 403
    resp_dc = client.post(
        "/api/v1/limits/dc-dc",
        json={"source_dc": "dc1", "target_dc": "dc2", "limit_mb_per_sec": 50.0},
        headers=user_headers,
    )
    assert resp_dc.status_code == 403

    # 4. Попытка изменить лимит HDFS-HDFS -> 403
    resp_hdfs = client.post(
        "/api/v1/limits/hdfs-hdfs",
        json={"source_cluster": "demo-cluster", "target_cluster": "backup-cluster", "limit_mb_per_sec": 20.0},
        headers=user_headers,
    )
    assert resp_hdfs.status_code == 403

    # 5. Попытка изменить лимит через PUT /tokens/limit -> 403
    resp_tokens = client.put(
        "/tokens/limit",
        json={"limit_bytes_per_sec": 20 * 1024 * 1024},
        headers=user_headers,
    )
    assert resp_tokens.status_code == 403

    # 6. Авторизуемся под администратором admin_user
    admin_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    assert admin_resp.status_code == 200
    admin_token = admin_resp.json()["access_token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    # 7. Администратор успешно меняет глобальный лимит -> 200
    resp_admin_global = client.post(
        "/api/v1/limits/global",
        json={"limit_bytes_per_sec": 100 * 1024 * 1024},
        headers=admin_headers,
    )
    assert resp_admin_global.status_code == 200

    # 8. Администратор успешно меняет лимит DC-DC -> 200
    resp_admin_dc = client.post(
        "/api/v1/limits/dc-dc",
        json={"source_dc": "dc1", "target_dc": "dc2", "limit_mb_per_sec": 80.0},
        headers=admin_headers,
    )
    assert resp_admin_dc.status_code == 200

    # 9. Администратор успешно меняет лимит HDFS-HDFS -> 200
    resp_admin_hdfs = client.post(
        "/api/v1/limits/hdfs-hdfs",
        json={"source_cluster": "demo-cluster", "target_cluster": "backup-cluster", "limit_mb_per_sec": 40.0},
        headers=admin_headers,
    )
    assert resp_admin_hdfs.status_code == 200


def test_favicon(client):
    """Проверка отдачи фавиконки страницы."""
    resp_svg = client.get("/favicon.svg")
    assert resp_svg.status_code == 200
    assert "image/svg+xml" in resp_svg.headers.get("content-type", "")

    resp_ico = client.get("/favicon.ico")
    assert resp_ico.status_code == 200


def test_job_lifecycle_actions_start_stop_edit_delete(client):
    """Тестирование полного жизненного цикла управления задачей: старт, стоп, редактирование и удаление."""
    # 1. Авторизуемся под admin_user
    admin_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    admin_token = admin_resp.json()["access_token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    # 2. Создаем задачу
    created = client.post(
        "/jobs",
        json={
            "source_path": "/data/test_src",
            "target_path": "/data/test_dst",
            "source_cluster_id": "demo-cluster",
            "target_cluster_id": "backup-cluster",
            "run_as_service_account": True,
        },
        headers=admin_headers,
    ).json()
    job_id = created["id"]
    assert created["status"] == "QUEUED"

    # 3. Активную задачу (QUEUED) нельзя повторно запускать, редактировать или удалять
    resp_re_start = client.post(f"/jobs/{job_id}/start", headers=admin_headers)
    assert resp_re_start.status_code == 400

    resp_edit_active = client.put(
        f"/jobs/{job_id}",
        json={"source_path": "/data/test_src_edited"},
        headers=admin_headers,
    )
    assert resp_edit_active.status_code == 400

    resp_del_active = client.delete(f"/jobs/{job_id}", headers=admin_headers)
    assert resp_del_active.status_code == 400

    # 4. Остановка задачи (POST /jobs/{id}/stop)
    stop_resp = client.post(f"/jobs/{job_id}/stop", headers=admin_headers)
    assert stop_resp.status_code == 200
    assert stop_resp.json()["status"] == "CANCELLED"

    # 5. Редактирование остановленной задачи (PUT /jobs/{id}) -> успешно
    edit_resp = client.put(
        f"/jobs/{job_id}",
        json={
            "source_path": "/data/test_src_edited",
            "target_path": "/data/test_dst_edited",
            "is_scheduled": True,
            "cron_expression": "@hourly",
        },
        headers=admin_headers,
    )
    assert edit_resp.status_code == 200
    updated_job = edit_resp.json()
    assert updated_job["source_path"] == "/data/test_src_edited"
    assert updated_job["target_path"] == "/data/test_dst_edited"
    assert updated_job["is_scheduled"] is True
    assert updated_job["next_run_at"] is not None

    # 6. Запуск остановленной задачи (POST /jobs/{id}/start) -> успешно
    start_resp = client.post(f"/jobs/{job_id}/start", headers=admin_headers)
    assert start_resp.status_code == 200
    restarted_job = start_resp.json()
    assert restarted_job["status"] == "QUEUED"
    assert restarted_job["copied_bytes"] == 0

    # 7. Останавливаем перед удалением и удаляем
    client.post(f"/jobs/{job_id}/stop", headers=admin_headers)
    del_resp = client.delete(f"/jobs/{job_id}", headers=admin_headers)
    assert del_resp.status_code == 200
    assert del_resp.json()["status"] == "deleted"

    # 8. Проверяем, что задача удалена
    get_resp = client.get(f"/jobs/{job_id}", headers=admin_headers)
    assert get_resp.status_code == 404
