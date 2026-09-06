import pytest
from app.models.change_requests import ChangeRequestCreate
from app.models.yarn import DraftQueueItem, PartitionResourceConfig
from app.services.storage import StorageService


@pytest.fixture
def temp_storage(tmp_path):
    db_file = str(tmp_path / "test_yarn_explorer.db")
    return StorageService(db_path=db_file)


def test_storage_crud(temp_storage):
    part = PartitionResourceConfig(
        partition_name="DEFAULT",
        capacity=50.0,
        max_capacity=80.0,
        is_elastic=True,
        elasticity_ratio=1.6,
        memory_mb=1048576,
        vcores=512,
    )
    draft = DraftQueueItem(
        path="root.production",
        name="production",
        parent_path="root",
        action="modify",
        is_leaf=True,
        state="RUNNING",
        partitions={"DEFAULT": part},
    )

    # 1. Создание заявки
    cr_id = temp_storage.create_change_request(
        cluster_id="prod-yarn",
        title="Расширение квоты production",
        description="Требуется для запуска ночного пайплайна",
        author="writer_user",
        changes=[draft],
        diffs=[],
    )
    assert cr_id > 0
    assert temp_storage.count_pending() == 1

    # 2. Получение заявки
    cr = temp_storage.get_change_request(cr_id)
    assert cr is not None
    assert cr.id == cr_id
    assert cr.title == "Расширение квоты production"
    assert cr.status == "SUBMITTED"
    assert cr.author == "writer_user"
    assert len(cr.changes) == 1

    # 3. Список заявок
    items = temp_storage.list_change_requests(cluster_id="prod-yarn")
    assert len(items) == 1
    assert items[0].id == cr_id

    # 4. Одобрение заявки
    approved = temp_storage.approve_change_request(
        cr_id=cr_id,
        reviewer="admin_user",
        comment="Одобрено на созвоне архитектуры",
        xml_content="<configuration></configuration>",
    )
    assert approved is True

    cr_approved = temp_storage.get_change_request(cr_id)
    assert cr_approved.status == "APPROVED"
    assert cr_approved.reviewer == "admin_user"
    assert cr_approved.review_comment == "Одобрено на созвоне архитектуры"
    assert cr_approved.xml_content == "<configuration></configuration>"
    assert temp_storage.count_pending() == 0

    # 5. Создание второй заявки и отклонение
    cr_id_2 = temp_storage.create_change_request(
        cluster_id="prod-yarn",
        title="Некорректная заявка",
        description="Тест",
        author="writer_user",
        changes=[draft],
        diffs=[],
    )
    rejected = temp_storage.reject_change_request(
        cr_id=cr_id_2,
        reviewer="admin_user",
        comment="Превышен бюджет кластера",
    )
    assert rejected is True

    cr_rejected = temp_storage.get_change_request(cr_id_2)
    assert cr_rejected.status == "REJECTED"
    assert cr_rejected.review_comment == "Превышен бюджет кластера"

    # 6. Отзыв заявки
    cr_id_3 = temp_storage.create_change_request(
        cluster_id="prod-yarn",
        title="Заявка для отзыва",
        description="Сам передумал",
        author="writer_user",
        changes=[draft],
        diffs=[],
    )
    cancelled = temp_storage.cancel_change_request(cr_id=cr_id_3, author="writer_user")
    assert cancelled is True
    cr_cancelled = temp_storage.get_change_request(cr_id_3)
    assert cr_cancelled.status == "CANCELLED"


def test_storage_sqlalchemy_url():
    storage = StorageService(db_url="sqlite:///:memory:")
    assert storage.is_token_revoked("jti-1") is False
    assert storage.revoke_token("jti-1", "2099-01-01T00:00:00Z") is True
    assert storage.is_token_revoked("jti-1") is True

    # Rate limiting
    allowed, retry = storage.check_and_record_rate_limit("ip:127.0.0.1", max_requests=2, window_seconds=60)
    assert allowed is True
    allowed, retry = storage.check_and_record_rate_limit("ip:127.0.0.1", max_requests=2, window_seconds=60)
    assert allowed is True
    allowed, retry = storage.check_and_record_rate_limit("ip:127.0.0.1", max_requests=2, window_seconds=60)
    assert allowed is False
    assert retry > 0
