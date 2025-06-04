from sqlmodel import Session, select
from db.models.scheme import Job, JobStatus
from db.repositories.base import BaseRepository
from typing import List, Optional
import uuid

class JobRepository(BaseRepository[Job]):
    def __init__(self, session: Session):
        super().__init__(session, Job)

    def create_job(self, user_id: uuid.UUID) -> Job:
        job = Job(user_id=user_id)
        return self.create(job)

    def get_jobs_by_user(self, user_id: uuid.UUID) -> List[Job]:
        statement = select(Job).where(Job.user_id == user_id)
        return self.session.exec(statement).all()

    def update_job_status(self, job_id: uuid.UUID, status: JobStatus) -> Optional[Job]:
        job = self.get_by_id(job_id)
        if job:
            job.status = status
            self.session.commit()
            self.session.refresh(job)
        return job