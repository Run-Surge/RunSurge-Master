from sqlalchemy.orm import Session, joinedload
from sqlalchemy import select
from sqlalchemy.dialects import postgresql
from app.db.models.scheme import Task, Data, Node, TaskDataDependency
from app.schemas.task import TaskCreate, TaskUpdate, TaskDataWithNodeInfo, TaskOutputDependentInfo
from app.db.repositories.base import BaseRepository
from fastapi import Depends
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

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
    
    async def get_task_dependencies(self, task_id: int):
        return await self.session.execute(select(Data).where(Data.task_id == task_id, Data.status == DataStatus.pending))

    async def get_task_dependents_info(self, task_id: int) -> List[TaskOutputDependentInfo]:
        """
        Complex join query to get output files from a task and the tasks that depend on them with node information.
        
        Query structure:
        1. Find all Data records where parent_task_id = task_id (output files)
        2. Join with TaskDataDependency to find dependent tasks
        3. Join with Task table to get dependent task details
        4. Left join with Node table to get node information (may be null if not assigned yet)
        
        Returns: List of TaskOutputDependencyInfo objects
        """
        query = (
            select(
                # Output file information
                Data.data_id,
                Data.file_name,
                Data.parent_task_id,
                
                # Dependent task information
                Task.task_id.label('dependent_task_id'),
                Task.status.label('dependent_task_status'),
                
                # Node information (may be null if task not assigned yet)
                Task.node_id,
                Node.ip_address.label('node_ip_address'),
                Node.port.label('node_port')
            )
            .select_from(Data)
            .join(TaskDataDependency, Data.data_id == TaskDataDependency.data_id)
            .join(Task, TaskDataDependency.task_id == Task.task_id)
            .outerjoin(Node, Task.node_id == Node.node_id)  # Left join because task may not be assigned to node yet
            .where(Data.parent_task_id == task_id)
        )
        
        result = await self.session.execute(query)
        rows = result.fetchall()
        
        return [
            TaskOutputDependentInfo(
                # Output file information
                data_id=row.data_id,
                file_name=row.file_name,
                parent_task_id=row.parent_task_id,
                
                # Dependent task information
                dependent_task_id=row.dependent_task_id,
                dependent_task_status=row.dependent_task_status,
                
                # Node information (may be None)
                node_id=row.node_id,
                node_ip_address=row.node_ip_address,
                node_port=row.node_port
            )
            for row in rows
        ]

    async def get_task_with_data_files(self, task_id: int):
        query = (
            select(Task)
            .options(joinedload(Task.data_files))
            .where(Task.task_id == task_id)
        )
        result = await self.session.execute(query)
        return result.unique().scalar_one_or_none()
    
    async def get_tasks_by_node_id(self, node_id: int) -> List[Task]:
        query = select(Task).where(Task.node_id == node_id)
        result = await self.session.execute(query)
        return result.scalars().all()

async def get_task_repository(session: AsyncSession = Depends(get_db)) -> TaskRepository:
    return TaskRepository(session)