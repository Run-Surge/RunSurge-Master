from sqlalchemy.orm import Session
from sqlalchemy import select, or_
from app.db.models.scheme import User
from app.db.repositories.base import BaseRepository
from typing import Optional
from passlib.hash import bcrypt_sha256
from fastapi import Depends
from app.db.session import get_db
from app.schemas.user import UserCreate, UserRead

class UserRepository(BaseRepository[User]):
    def __init__(self, session: Session):
        super().__init__(session, User)

    async def get_by_username(self, username: str) -> Optional[User]:
        statement = select(User).where(User.username == username)
        return self.session.execute(statement).scalar_one_or_none()

    async def get_by_email(self, email: str) -> Optional[User]:
        statement = select(User).where(User.email == email)
        return self.session.execute(statement).scalar_one_or_none()

    async def create_user(self, user: UserCreate) -> User:
        hashed_password = bcrypt_sha256.hash(user.password)
        db_user = User(username=user.username, email=user.email, password=hashed_password)
        return await self.create(db_user)

    async def username_or_email_exists(self, username: str, email: str) -> bool:
        statement = select(User).where(
            or_(User.username == username, User.email == email)
        )
        result = await self.session.execute(statement)
        return result.first() is not None
    


def get_user_repository(session: Session = Depends(get_db)) -> UserRepository:
    return UserRepository(session)