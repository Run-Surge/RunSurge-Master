from sqlalchemy.orm import Session
from sqlalchemy import select, update
from app.db.models.scheme import Job, JobStatus, JobType, Data, Task
from app.db.repositories.base import BaseRepository
from app.schemas.job import JobCreate, ComplexJobCreate
from typing import List, Optional
from fastapi import Depends
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

class JobRepository(BaseRepository[Job]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Job)
    async def create_job(self, job_data: JobCreate, user_id: int) -> Job:
        job = Job(
            user_id=user_id,
            job_name=job_data.job_name,
            job_type=job_data.job_type,
            script_name=job_data.script_name
        )
        
        return await self.create(job)

    async def get_simple_user_jobs(self, user_id: int) -> List[Job]:
        statement = select(Job).where(Job.user_id == user_id, Job.job_type == JobType.simple)
        result = await self.session.execute(statement)
        return result.scalars().all()

    async def update_job_status(self, job_id: int, status: JobStatus) -> Optional[Job]:
        job = await self.get_by_id(job_id)
        if job:
            job.status = status
            await self.session.commit()
            await self.session.refresh(job)
        return job
    
    async def update_complex_job_ram_and_status(self, job_id: int, job: Job) -> Optional[Job]:
        db_job = await self.get_by_id(job_id)
        if db_job:
            db_job.required_ram = job.required_ram
            db_job.status=job.status
            await self.session.commit()
            await self.session.refresh(job)
        return db_job

    async def update_job_script_path(self, job_id: int, script_path: str) -> Optional[Job]:
        job = await self.get_by_id(job_id)
        if job:
            job.script_path = script_path
            await self.session.commit()
            await self.session.refresh(job)
        return job
    async def get_pending_jobs(self) -> List[Job]:
        statement = select(Job).where(Job.status == JobStatus.pending_schedule)
        result = await self.session.execute(statement)
        return result.scalars().all()
    
    async def get_job(self, job_id: int) -> Optional[Job]:
        statement = select(Job).where(Job.job_id == job_id).options(joinedload(Job.output_data_file), joinedload(Job.payment))
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()
    
    async def create_complex_job(self, job_data: ComplexJobCreate) -> Job:
        job = Job(
            user_id=job_data.user_id,
            job_name=job_data.job_name,
            job_type=job_data.job_type,
            group_id=job_data.group_id, 
        )
        return await self.create(job)

    async def get_job_with_output_node(self, job_id: int) -> Optional[Job]:
        statement = (
            select(Job).where(Job.job_id == job_id)
            .options(
                joinedload(Job.output_data_file)
                .joinedload(Data.parent_task)
                .joinedload(Task.node)
                )   
        )
        result = await self.session.execute(statement)
        return result.unique().scalar_one_or_none()

    async def get_job_with_tasks(self, job_id: int) -> Optional[Job]:
        statement = select(Job).where(Job.job_id == job_id).options(joinedload(Job.tasks).joinedload(Task.earning))
        result = await self.session.execute(statement)
        return result.unique().scalar_one_or_none()

    async def update_job_output_data_id(self, job_id: int, data_id: int):
        update_statement = update(Job).where(Job.job_id == job_id).values(output_data_id=data_id)
        await self.session.execute(update_statement)
        await self.session.commit()

async def get_job_repository(session: AsyncSession = Depends(get_db)) -> JobRepository:
    return JobRepository(session)