from fastapi import Depends, HTTPException, APIRouter, Form
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.schemas.job import JobRead
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.job import get_job_service
from fastapi import UploadFile, File
from app.core.security import get_current_user_from_cookie
from app.db.models.scheme import User, JobType
from app.utils.constants import JOBS_DIRECTORY_PATH
from Parallelization.Parallelizer import Parallelizer
router = APIRouter()

@router.post("/", response_model=JobRead)
async def create_job(
    job_name: str = Form(...),
    job_type: JobType = Form(...),
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db), 
    current_user = Depends(get_current_user_from_cookie)
):
    job_service = get_job_service(session)
    job = await job_service.create_job_with_script(
        user_id=current_user["user_id"],
        file=file,
        job_name=job_name,
        job_type=job_type
    )
    try:
        Parallelizer(job.script_path,job.job_id)
        print("finished parallelization")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    return job


