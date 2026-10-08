"""Тесты для Фазы 4: Изоляция Kerberos (KerberosContextManager)."""

import os
import tempfile
import pytest

from backend.replicator.security.kerberos import (
    DEFAULT_SERVICE_PRINCIPAL,
    KerberosAuthenticationError,
    KerberosContextManager,
    open_hdfs_stream_with_kerberos,
)


def test_kerberos_context_manager_lifecycle():
    """Проверка полного жизненного цикла изоляции KRB5CCNAME."""
    orig_ccname = os.environ.get("KRB5CCNAME")
    temp_dir = tempfile.mkdtemp(prefix="krb5_test_")

    try:
        km = KerberosContextManager(
            principal="user_bob@COMPANY.CORP",
            cache_dir=temp_dir,
            mock=True,
        )

        assert os.environ.get("KRB5CCNAME") == orig_ccname

        with km as ctx:
            active_ccname = os.environ.get("KRB5CCNAME")
            assert active_ccname is not None
            assert active_ccname.startswith(temp_dir)
            assert os.path.exists(active_ccname)
            assert ctx.principal == "user_bob@COMPANY.CORP"

        # После выхода из контекста переменная окружения должна восстановиться,
        # а файл кэша должен быть удален
        assert os.environ.get("KRB5CCNAME") == orig_ccname
        assert not os.path.exists(active_ccname)

    finally:
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)


def test_kerberos_service_account_mode():
    """Проверка запуска от системной техучетки по умолчанию."""
    temp_dir = tempfile.mkdtemp(prefix="krb5_sa_")
    try:
        with KerberosContextManager(run_as_service_account=True, cache_dir=temp_dir, mock=True) as ctx:
            assert ctx.run_as_service_account is True
            assert ctx.principal == DEFAULT_SERVICE_PRINCIPAL
            assert os.path.exists(ctx.cache_file)
    finally:
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)


def test_kerberos_multi_tenant_isolation():
    """Проверка изоляции тикетов между разными пользователями/задачами."""
    temp_dir = tempfile.mkdtemp(prefix="krb5_multi_")
    try:
        km_sa = KerberosContextManager(run_as_service_account=True, cache_dir=temp_dir, mock=True)
        km_user = KerberosContextManager(principal="analyst_alice@REALM", cache_dir=temp_dir, mock=True)

        with km_sa as ctx1:
            ccname_sa = os.environ.get("KRB5CCNAME")
            assert "analyst_alice" not in ctx1.principal

            # Вложенный или параллельный контекст другого пользователя
            with km_user as ctx2:
                ccname_user = os.environ.get("KRB5CCNAME")
                assert ccname_user != ccname_sa
                assert ctx2.principal == "analyst_alice@REALM"

            # После выхода из вложенного контекста восстановился контекст техучетки
            assert os.environ.get("KRB5CCNAME") == ccname_sa
    finally:
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)


@pytest.mark.asyncio
async def test_kerberos_async_context_manager():
    """Проверка асинхронного вызова `async with KerberosContextManager`."""
    temp_dir = tempfile.mkdtemp(prefix="krb5_async_")
    try:
        async with KerberosContextManager(
            principal="async_worker@REALM",
            cache_dir=temp_dir,
            mock=True,
        ) as ctx:
            assert os.environ.get("KRB5CCNAME") == ctx.cache_file
            assert os.path.exists(ctx.cache_file)
    finally:
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)


def test_missing_keytab_error_non_mock():
    """Проверка генерации ошибки KerberosAuthenticationError при отсутствии keytab файла."""
    with pytest.raises(KerberosAuthenticationError, match="Keytab-файл не найден"):
        with KerberosContextManager(
            keytab="/non/existent/keytab/file.keytab",
            principal="real_user@REALM",
            mock=False,
        ):
            pass


def test_open_hdfs_stream_with_kerberos_mock():
    """Проверка хелпера вызова чтения PyArrow с изоляцией Kerberos."""
    # В mock-режиме pyarrow попытается подключиться или вернет None/ошибку соединения,
    # но контекст Kerberos должен корректно отработать и очиститься
    res = open_hdfs_stream_with_kerberos(
        hdfs_host="localhost",
        hdfs_port=8020,
        hdfs_path="/test/path",
        run_as_service_account=True,
        mock=True,
    )
    # PyArrow не сможет подключиться к фиктивному localhost:8020, поэтому вернет None или бросит ожидаемое исключение
    # Главное - отсутствие необработанных сбоев изоляции
    assert res is None or hasattr(res, "read")


def test_kerberos_impersonation_user_extraction():
    """Проверка извлечения doAs пользователя для Apache Ranger аудита."""
    # 1. Обычный пользовательский принципал
    km1 = KerberosContextManager(principal="alice@COMPANY.CORP", run_as_service_account=False, mock=True)
    assert km1.impersonate_user == "alice"
    assert km1.principal == "alice@COMPANY.CORP"

    # 2. Принципал с инстансом (например, admin)
    km2 = KerberosContextManager(principal="bob/admin@REALM.LOCAL", run_as_service_account=False, mock=True)
    assert km2.impersonate_user == "bob"

    # 3. Системная техучетка (без имперсонации)
    km3 = KerberosContextManager(run_as_service_account=True, mock=True)
    assert km3.impersonate_user is None

    # 4. Явно переданный impersonate_user
    km4 = KerberosContextManager(impersonate_user="charlie_custom", mock=True)
    assert km4.impersonate_user == "charlie_custom"


def test_pyarrow_hdfs_impersonation_call(monkeypatch):
    """Проверка передачи doAs user и kerb_ticket в pyarrow.fs.HadoopFileSystem."""
    from unittest.mock import MagicMock
    import pyarrow.fs

    mock_fs_instance = MagicMock()
    mock_hadoop_fs_class = MagicMock(return_value=mock_fs_instance)

    monkeypatch.setattr(pyarrow.fs, "HadoopFileSystem", mock_hadoop_fs_class)

    res = open_hdfs_stream_with_kerberos(
        hdfs_host="nn01.hadoop.local",
        hdfs_port=8020,
        hdfs_path="/lake/events/data.parquet",
        principal="analyst_daria@CORP.LOCAL",
        run_as_service_account=False,
        mock=True,
    )

    mock_hadoop_fs_class.assert_called_once()
    args, kwargs = mock_hadoop_fs_class.call_args
    assert args[0] == "nn01.hadoop.local"
    assert kwargs.get("port") == 8020
    assert kwargs.get("user") == "analyst_daria"
    assert "krb5cc_repl_" in kwargs.get("kerb_ticket")
    mock_fs_instance.open_input_stream.assert_called_once_with("/lake/events/data.parquet")
    assert res == mock_fs_instance.open_input_stream.return_value
