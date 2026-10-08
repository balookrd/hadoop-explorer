"""Утилиты для надежного разрешения путей к корневой директории данных data/ платформы.

Гарантирует, что все сервисы (YARN, Spark, SQL, HDFS, Replicator) сохраняют
SQLite базы данных и кэш результатов (results) в единую корневую директорию hadoop-explorer/data,
независимо от того, из какой рабочей директории (CWD) запущен uvicorn, pytest или демон.
"""

import os
from pathlib import Path


def get_repo_root() -> Path:
    """Определяет абсолютный путь к корню монорепозитория hadoop-explorer."""
    # Если запущено внутри стандартного контейнера Docker (/app)
    if os.path.exists("/app/data"):
        return Path("/app")

    current = Path(__file__).resolve()
    # Поднимаемся вверх и ищем корень с директориями backend и frontend или data
    for parent in current.parents:
        if (parent / "backend").is_dir() and (parent / "frontend").is_dir():
            return parent
        if (parent / "backend").is_dir() and (parent / "data").is_dir():
            return parent

    # Fallback на текущую рабочую директорию
    return Path.cwd().resolve()


def get_data_dir() -> Path:
    """Возвращает путь к канонической корневой директории данных платформы: hadoop-explorer/data/."""
    env_data_dir = os.environ.get("HADOOP_EXPLORER_DATA_DIR")
    if env_data_dir:
        p = Path(env_data_dir).resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p

    if os.path.exists("/app/data"):
        p = Path("/app/data")
        p.mkdir(parents=True, exist_ok=True)
        return p

    root = get_repo_root()
    p = root / "data"
    p.mkdir(parents=True, exist_ok=True)
    return p.resolve()


def resolve_db_url(
    default_filename: str,
    async_driver: bool = False,
    env_var: str | None = None,
) -> str:
    """Вычисляет строку подключения к SQLite базе данных в единой папке data/.

    Args:
        default_filename: Имя файла базы данных (например, 'yarn_explorer.db').
        async_driver: Использовать ли префикс 'sqlite+aiosqlite:///' для async SQLAlchemy.
        env_var: Имя переменной окружения с URL БД (например, 'DATABASE_URL' или 'REPLICATOR_DATABASE_URL').

    Returns:
        Строка подключения SQLAlchemy URL.
    """
    if env_var and os.environ.get(env_var):
        url = os.environ[env_var]
        if url == ":memory:":
            return "sqlite+aiosqlite:///:memory:" if async_driver else "sqlite:///:memory:"
        return url

    # Общая переменная DATABASE_URL
    if os.environ.get("DATABASE_URL"):
        url = os.environ["DATABASE_URL"]
        if url == ":memory:":
            return "sqlite+aiosqlite:///:memory:" if async_driver else "sqlite:///:memory:"
        return url

    data_dir = get_data_dir()
    db_path = data_dir / default_filename
    prefix = "sqlite+aiosqlite:///" if async_driver else "sqlite:///"
    return f"{prefix}{db_path.resolve()}"
