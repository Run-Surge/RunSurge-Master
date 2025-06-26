

from app.db.repositories.base import BaseRepository
from app.db.models.scheme import Data, InputData, Task, TaskDataDependency, Job
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends
from app.db.session import get_db
from protos import master_pb2
from sqlalchemy import select
from typing import Optional


class DataRepository(BaseRepository[Data]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Data)

    async def create_data(self, file_name: str, parent_task_id: Optional[int] = None):
        if parent_task_id is None:
            data = Data(file_name=file_name)
        else:
            data = Data(file_name=file_name, parent_task_id=parent_task_id)
        return await self.create(data)  
    
    async def get_data_job_by_parent_task(self, data_id: int):
        statement = (
            select(Job)
            .join(Task, Task.job_id == Job.job_id)
            .join(Data, Data.parent_task_id == Task.task_id)
            .where(Data.data_id == data_id)
            .limit(1)
        )
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()
    
    async def get_data_job_by_dependent_tasks(self, data_id: int):
        statement = (
            select(Job)
            .join(Task, Task.job_id == Job.job_id)
            .join(TaskDataDependency, TaskDataDependency.task_id == Task.task_id)
            .where(TaskDataDependency.data_id == data_id)
            .limit(1)
        )
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()

    async def update_data_metadata(self, data_id: int, parent_id: int, data_request: master_pb2.MasterDataNotification):
        statement = select(Data).where(Data.data_id == data_id, Data.parent_task_id == parent_id)
        result = await self.session.execute(statement)
        data = result.scalar_one_or_none()
        if data:
            data.size = data_request.size
            data.hash = data_request.hash
            return await self.update(data)
        return None



class InputDataRepository(BaseRepository[InputData]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, InputData)
    
    async def create_input_data(self, job_id: int, chunk_index: int, total_chunks: int,file_name: str):
        input_data = InputData(job_id=job_id, chunk_index=chunk_index, total_chunks=total_chunks,file_name=file_name)
        return await self.create(input_data)
    
    async def update_input_data(self, input_data: InputData):
        return await self.update(input_data)
    
    async def get_input_data(self, job_id: int):
        statement = select(InputData).where(InputData.job_id == job_id)
        result = await self.session.execute(statement)
        input_data = result.scalar_one_or_none()
        return input_data
    



     

    