from sqlalchemy.orm import Session
from sqlalchemy import select
from app.db.models.scheme import Task
from app.schemas.task import TaskCreate, TaskUpdate
from app.db.repositories.base import BaseRepository
from fastapi import Depends
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

class TaskRepository(BaseRepository[Task]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Task)
    async def create_task(self, task_data: TaskCreate) -> Task:
        task = Task(
            job_id=task_data.job_id,
            data_id=task_data.data_id,
            required_ram=task_data.required_ram,
        )
        return await self.create(task)
    
async def get_task_repository(session: AsyncSession = Depends(get_db)) -> TaskRepository:
    return TaskRepository(session)