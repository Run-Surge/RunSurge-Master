import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from urllib.parse import quote_plus
from app.core.config import settings
from contextlib import asynccontextmanager
from typing import AsyncGenerator

engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False, 
    bind=engine,
    class_=AsyncSession
)

async def init_db():
    from app.db.models.scheme import Base
    print("Database URL:", settings.DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Database initialized successfully.")

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

@asynccontextmanager
async def get_db_context() -> AsyncGenerator[AsyncSession, None]:
    """
    An async context manager that provides a database session.
    It yields a DB session and ensures it's closed upon exiting the 'async with' block.
    
    gets used in gRPC server
    usage:
    async with get_db_context() as session:
        user_service = get_user_service(session)
        user = await user_service.get_user_by_id(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found.")
        return user
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            print("Closing database session")
            await session.close()