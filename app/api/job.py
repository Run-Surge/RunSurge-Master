from fastapi import Depends, HTTPException, APIRouter, Form
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.db.repositories.job import JobRepository, get_job_repository
from app.schemas.job import JobRead, JobCreate
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.job import get_job_service
from typing import Optional
from fastapi import UploadFile, File
from app.core.security import get_current_user
from app.db.models.scheme import User
from app.utils.constants import JOBS_DIRECTORY_PATH
from Parallelization.Parallelizer import Parallelizer
from app.utils.worker import task_queue
import logging
router = APIRouter()

@router.post("/", response_model=JobRead)
async def create_job(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db), 
    current_user: User = Depends(get_current_user)
):
    global task_queue
    job_service = get_job_service(session)
    job = await job_service.create_job_with_script(
        user_id=current_user["user_id"],
        file=file,
    )
    path_name=f"{JOBS_DIRECTORY_PATH}/{job.job_id}/{job.script_name}.py"
    Parallelizer(path_name,job.job_id)
    print("finished parallelization")
    task_queue.put((job.job_id,))  # the comma to make it a tuple
    return job


