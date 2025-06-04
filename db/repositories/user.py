from sqlmodel import Session, select
from db.models.scheme import User
from db.repositories.base import BaseRepository
from typing import Optional
import uuid
from passlib.hash import bcrypt_sha256

class UserRepository(BaseRepository[User]):
    def __init__(self, session: Session):
        super().__init__(session, User)

    def get_by_username(self, username: str) -> Optional[User]:
        statement = select(User).where(User.username == username)
        return self.session.exec(statement).first()

    def get_by_email(self, email: str) -> Optional[User]:
        statement = select(User).where(User.email == email)
        return self.session.exec(statement).first()

    def create_user(self, username: str, email: str, password: str) -> User:
        hashed_password = bcrypt_sha256.hash(password)
        user = User(username=username, email=email, password=hashed_password)
        return self.create(user)

    def username_or_email_exists(self, username: str, email: str) -> bool:
        statement = select(User).where(
            (User.username == username) | (User.email == email)
        )
        return self.session.exec(statement).first() is not None