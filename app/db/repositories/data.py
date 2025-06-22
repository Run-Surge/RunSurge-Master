

from app.db.repositories.base import BaseRepository
from app.db.models.scheme import Data
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends
from app.db.session import get_db
from protos import master_pb2
from sqlalchemy import select
from typing import Optional


class DataRepository(BaseRepository[Data]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Data)

    async def create_data(self, file_name: str, job_id: int, parent_task_id: Optional[int] = None):
        if parent_task_id is None:
            data = Data(file_name=file_name, job_id=job_id)
        else:
            data = Data(file_name=file_name, job_id=job_id, parent_task_id=parent_task_id)
        return await self.create(data)  
    

    async def update_data_metadata(self, data_id: int, parent_id: int, data_request: master_pb2.MasterDataNotification):
        statement = select(Data).where(Data.data_id == data_id, Data.parent_task_id == parent_id)
        result = await self.session.execute(statement)
        data = result.scalar_one_or_none()
        if data:
            data.size = data_request.size
            data.hash = data_request.hash
            return await self.update(data)
        return None


     

    