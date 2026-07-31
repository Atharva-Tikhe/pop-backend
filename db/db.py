import asyncio
from db.model import Base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# TODO: Update with actual username/pass from .env
DATABASE_URL = "postgresql+asyncpg://admin:admin@localhost:5432/pipeline_db"

engine = create_async_engine(DATABASE_URL, echo=True)

SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)

async def init():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# asyncio.run(init())



