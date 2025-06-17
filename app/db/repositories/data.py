

from app.db.repositories.base import BaseRepository
from app.db.models.scheme import Data
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends
from app.db.session import get_db

class DataRepository(BaseRepository[Data]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Data)

    async def create_data(self, user_id: int):
        data = Data(user_id=user_id)
        return await self.create(data)
    

     

    