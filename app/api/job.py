from fastapi import Depends, HTTPException, APIRouter
from app.db.repositories.job import JobRepository, get_job_repository
from app.schemas.job import JobCreate, JobRead

router = APIRouter()

@router.post("/", response_model=JobRead)
async def create_job(job: JobCreate, job_repo: JobRepository = Depends(get_job_repository)):
    user = job_repo.get_by_id(job.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    
    return job_repo.create_job(job.user_id)
