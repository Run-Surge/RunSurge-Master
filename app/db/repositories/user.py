
from sqlalchemy import select, or_
from app.db.models.scheme import User
from app.db.repositories.base import BaseRepository
from typing import Optional
from fastapi import Depends
from app.db.session import get_db
from app.schemas.user import UserLoginCreate
from app.core.security import security_manager
from sqlalchemy.ext.asyncio import AsyncSession
class UserRepository(BaseRepository[User]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, User)

    async def get_by_id(self, user_id: int) -> Optional[User]:
        statement = select(User).where(User.user_id == user_id)
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()

    async def get_by_username(self, username: str) -> Optional[User]:
        statement = select(User).where(User.username == username)
        return await self.session.execute(statement).scalar_one_or_none()

    async def get_by_email(self, email: str) -> Optional[User]:
        statement = select(User).where(User.email == email)
        return await self.session.execute(statement).scalar_one_or_none()

    async def create_user(self, user: UserLoginCreate) -> User:
        hashed_password = security_manager.hash_password(user.password)
        db_user = User(username=user.username, email=user.email, password=hashed_password)
        return await self.create(db_user)

    async def username_or_email_exists(self, username: str, email: str) -> Optional[User]:
        statement = select(User).where(
            or_(User.username == username, User.email == email)
        )
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()
    


async def get_user_repository(session: AsyncSession = Depends(get_db)) -> UserRepository:
    return UserRepository(session)