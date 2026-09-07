import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from app.core.config import settings

Base = declarative_base()

db_url = settings.database.url
if db_url.startswith("sqlite+aiosqlite:///./"):
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data"))
    os.makedirs(data_dir, exist_ok=True)
    db_file = db_url.replace("sqlite+aiosqlite:///./", "")
    db_url = f"sqlite+aiosqlite:///{os.path.join(data_dir, os.path.basename(db_file))}"

engine = create_async_engine(db_url, echo=False, future=True)

AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    import app.models.models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
