from app.db.repositories.job import JobRepository
from app.schemas.job import JobCreate, JobUpdate
from fastapi import Depends, HTTPException
from app.db.repositories.job import get_job_repository
from typing import Optional
from fastapi import UploadFile
from app.utils.utils import Create_directory, save_file
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils.constants import JOBS_DIRECTORY_PATH, FILE_SIZE_LIMIT
from app.db.models.scheme import JobStatus, JobType

class JobService:
    def __init__(self, job_repo: JobRepository):
        self.job_repo = job_repo

    def validate_file(self, file: UploadFile):
        if not file.filename.endswith('.py'):
            raise HTTPException(status_code=400, detail="Only Python files (.py) are allowed")
        if file.size == 0:
            raise HTTPException(status_code=400, detail="File is empty")
        if file.size > FILE_SIZE_LIMIT:
            raise HTTPException(status_code=400, detail="File size exceeds 10MB limit") 
        
        
    async def create_job_with_script(
        self, 
        user_id: int,
        file: UploadFile,
        job_name: str,
        job_type: JobType
    ):
        try:
            self.validate_file(file)
            job_data = JobCreate(
                job_name=job_name,
                job_type=job_type,
                script_name=file.filename.split(".")[0]
            )
            job = await self.job_repo.create_job(job_data, user_id)        
            Create_directory(f"{JOBS_DIRECTORY_PATH}/{job.job_id}")
            random_name = str(uuid.uuid4())
            save_file(file, f"{JOBS_DIRECTORY_PATH}/{job.job_id}/{random_name}.py")
            await self.job_repo.update_job_script_path(job.job_id, f"{JOBS_DIRECTORY_PATH}/{job.job_id}/{random_name}.py")
            return job
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    async def get_job(self, job_id: int):
        return await self.job_repo.get_job(job_id)

    async def update_job(self, job_id: int, job: JobUpdate):
        return await self.job_repo.update_job(job_id, job)
    
    async def get_user_jobs(self, user_id: int):
        return await self.job_repo.get_jobs_by_user(user_id)
    
    async def get_jobs_not_scheduled(self):
        return await self.job_repo.get_pending_jobs()
    
    async def update_job_status(self, job_id: int, status: JobStatus):
        return await self.job_repo.update_job_status(job_id, status)
    
def get_job_service(session: AsyncSession) -> JobService:
    return JobService(JobRepository(session))