from app.db.repositories.user import UserRepository
from app.schemas.user import UserCreate
from fastapi import HTTPException
from fastapi import Depends
from app.db.repositories.user import get_user_repository

class UserService:
    def __init__(self, user_repo: UserRepository):
        self.user_repo = user_repo

    async def create_user(self, user: UserCreate):
        if await self.user_repo.username_or_email_exists(user.username, user.email):
            raise HTTPException(status_code=400, detail="Username or email already exists")
        
        return await self.user_repo.create_user(user)

def get_user_service(user_repo: UserRepository = Depends(get_user_repository)) -> UserService:
    return UserService(user_repo)
    