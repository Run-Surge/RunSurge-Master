from sqlalchemy.orm import Session
from sqlalchemy import select
from app.db.models.scheme import Job, JobStatus
from app.db.repositories.base import BaseRepository
from typing import List, Optional
from fastapi import Depends
from app.db.session import get_db

class JobRepository(BaseRepository[Job]):
    def __init__(self, session: Session):
        super().__init__(session, Job)

    async def create_job(self, user_id: int) -> Job:
        job = Job(user_id=user_id)
        return await self.create(job)

    async def get_jobs_by_user(self, user_id: int) -> List[Job]:
        statement = select(Job).where(Job.user_id == user_id)
        return await self.session.execute(statement).scalars().all()

    async def update_job_status(self, job_id: int, status: JobStatus) -> Optional[Job]:
        job = await self.get_by_id(job_id)
        if job:
            job.status = status
            await self.session.commit()
            await self.session.refresh(job)
        return job
    
    
def get_job_repository(session: Session = Depends(get_db)) -> JobRepository:
    return JobRepository(session)