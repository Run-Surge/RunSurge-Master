from app.db.repositories.user import UserRepository
from app.schemas.user import UserLoginCreate
from fastapi import HTTPException
from fastapi import Depends
from app.db.repositories.user import get_user_repository
from typing import Optional
from app.db.models.scheme import User
from app.schemas.user import UserLoginCreate
from app.core.security import security_manager
from sqlalchemy.ext.asyncio import AsyncSession

class UserService:
    def __init__(self, user_repo: UserRepository):
        self.user_repo = user_repo


    async def get_user_by_id(self, user_id: int):
        return await self.user_repo.get_by_id(int(user_id))

    async def create_user(self, user: UserLoginCreate):
        if await self.user_repo.username_or_email_exists(user.username, user.email):
            raise HTTPException(status_code=400, detail="Username or email already exists")
        
        return await self.user_repo.create_user(user)
    
    
    async def user_exists(self, username: str, email: str) -> Optional[User]:
        return await self.user_repo.username_or_email_exists(username, email)
    
    async def login_user(self, user: UserLoginCreate):
        db_user = await self.user_repo.username_or_email_exists(user.username_or_email, user.username_or_email)
        if not db_user:
            raise HTTPException(status_code=401, detail="Invalid username or email")
        if not security_manager.verify_password(user.password, db_user.password):
            raise HTTPException(status_code=401, detail="Invalid password")
        return db_user

def get_user_service(session: AsyncSession) -> UserService:
    """
        Just hides the abstraction of dependency injection
    """
    return UserService(UserRepository(session))