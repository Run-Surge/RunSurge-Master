from sqlalchemy.orm import Session
from sqlalchemy import select
from app.db.models.scheme import Task, Data
from app.schemas.task import TaskCreate, TaskUpdate
from app.db.repositories.base import BaseRepository
from fastapi import Depends
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

class TaskRepository(BaseRepository[Task]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Task)
    async def create_task(self, job_id: int, data_ids: list[int], required_ram: int, node_id: int) -> Task:
        task = Task(
            node_id=node_id,
            job_id=job_id,
            required_ram=required_ram,
        )
        for data_id in data_ids:
            data = await self.session.execute(select(Data).where(Data.data_id == data_id))
            data = data.scalar_one_or_none()
            if data:
                task.data_dependencies.append(data)
        return await self.create(task)
    
async def get_task_repository(session: AsyncSession = Depends(get_db)) -> TaskRepository:
    return TaskRepository(session)