

from app.db.repositories.base import BaseRepository
from app.db.models.scheme import Data
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends
from app.db.session import get_db

class DataRepository(BaseRepository[Data]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Data)

    async def create_data(self, file_name: str, job_id: int, provider_id: int = -1):
        if provider_id == -1:
            data = Data(file_name=file_name, job_id=job_id)
        else:
            data = Data(file_name=file_name, job_id=job_id, provider_id=provider_id)    
        return await self.create(data)
    

     

    