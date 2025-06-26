from app.db.repositories.job import JobRepository
from app.schemas.job import JobCreate, ComplexJobCreate
from app.db.models.scheme import Job
from fastapi import Depends, HTTPException
from app.db.repositories.job import get_job_repository
from typing import Optional
from fastapi import UploadFile
from app.utils.utils import Create_directory, save_file
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils.constants import JOBS_DIRECTORY_PATH
from app.db.models.scheme import JobStatus, JobType, TaskStatus 
from app.utils.utils import validate_file, get_data_path
from app.services.worker_client import WorkerClient
import traceback
from protos import common_pb2
import logging
import os

class JobService:
    def __init__(self, job_repo: JobRepository):
        self.job_repo = job_repo
    async def create_job_with_script(
        self, 
        user_id: int,
        file: UploadFile,
        job_name: str,
        job_type: JobType
    ):
        try:
            validate_file(file)
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
        job = await self.job_repo.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return job

    async def update_job_ram_and_status(self, job_id: int, job: Job):
        return await self.job_repo.update_job_ram_and_status(job_id, job)
    
    async def get_simple_user_jobs(self, user_id: int):
        return await self.job_repo.get_simple_user_jobs(user_id)
    
    async def get_jobs_not_scheduled(self):
        return await self.job_repo.get_pending_jobs()
    
    async def update_job_status(self, job_id: int, status: JobStatus):
        return await self.job_repo.update_job_status(job_id, status)
    
    async def update_complex_job_ram_and_status(self, job_id: int, job: Job):
        return await self.job_repo.update_complex_job_ram_and_status(job_id, job)
        
    async def check_job_status(self, job_id: int):
        job = await self.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status != JobStatus.submitted:
            raise HTTPException(status_code=400, detail="Job is already running or completed")
    async def create_complex_job(self, user_id: int, job_name: str, job_type: JobType, group_id: int, script_path: str):
        try:
            job_data = ComplexJobCreate(
                user_id=user_id,
                job_name=job_name,
                job_type=job_type,
                group_id=group_id,
                script_path=script_path
                )
            job = await self.job_repo.create_complex_job(job_data)
            return job
        except Exception as e:
            print(traceback.format_exc())
            raise HTTPException(status_code=500, detail=str(e))
        
    async def download_output_data(self, job_id: int):
        try: 
            logging.info(f"Downloading job {job_id} output data")
            job = await self.job_repo.get_job_with_output_node(job_id)
            logging.info(f"Job {job_id} output data: {job}")
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
                    
            worker_client = WorkerClient()
            data_identifier = common_pb2.DataIdentifier(
                data_id=job.output_data_file.data_id,
            )   
            output_data_file = job.output_data_file
            node = output_data_file.parent_task.node
            await worker_client.stream_data(
                data_identifier,
                node.ip_address,
                node.port,
                get_data_path(output_data_file.file_name, job_id)
            )
            await self.job_repo.update_job_status(job_id, JobStatus.completed)
        except Exception as e:
            print(traceback.format_exc())
            raise HTTPException(status_code=500, detail=str(e))

    async def update_job_after_task_completion(self, job_id: int):
        try:
            logging.info(f"Updating job {job_id} after task completion")
            job = await self.job_repo.get_job_with_tasks(job_id)
            all_tasks_completed = True
            for task in job.tasks:
                if task.status != TaskStatus.completed:
                    all_tasks_completed = False
                    break

            logging.info(f"Job {job_id} all tasks completed: {all_tasks_completed}")
            if all_tasks_completed:
                await self.download_output_data(job_id)
                await self.update_job_status(job_id, JobStatus.completed)
            
        except Exception as e:
            print(traceback.format_exc())
            raise HTTPException(status_code=500, detail=str(e))
        
    async def get_output_file_path(self, job_id: int):
        job = await self.job_repo.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status != JobStatus.completed:
            raise HTTPException(status_code=400, detail="Job is not completed yet")        
        return os.path.join(JOBS_DIRECTORY_PATH, str(job_id), job.output_data_file.file_name)
    

    async def update_job_output_data_id(self, job_id: int, data_id: int) -> bool:
        try:
            await self.job_repo.update_job_output_data_id(job_id, data_id)
            return True
        except Exception as e:
            print(traceback.format_exc())
            return False

def get_job_service(session: AsyncSession) -> JobService:
    return JobService(JobRepository(session))