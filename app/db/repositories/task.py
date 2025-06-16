from sqlalchemy.orm import Session
from sqlalchemy import select
from app.db.models.scheme import Task, TaskStatus
from app.db.repositories.base import BaseRepository
from fastapi import Depends
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

class TaskRepository(BaseRepository[Task]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Task)
    # not tested due to data_id
    async def create_task(self, job_id: int, data_id: int, required_ram: int) -> Task:
        task = Task(
            job_id=job_id,
            data_id=data_id,
            required_ram=required_ram,
            status=TaskStatus.pending
        )
        return await self.create(task)
    
async def get_task_repository(session: AsyncSession = Depends(get_db)) -> TaskRepository:
    return TaskRepository(session)