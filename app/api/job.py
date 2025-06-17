from fastapi import Depends, HTTPException, APIRouter
from app.db.repositories.job import JobRepository, get_job_repository
from app.schemas.job import JobCreate, JobRead
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

@router.post("/", response_model=JobRead)
async def create_job(job: JobCreate, session: AsyncSession = Depends(get_db)):
    #TODO: use job service instead of job repository
    job_repo = JobRepository(session)
    user = await job_repo.get_by_id(job.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    return await job_repo.create_job(job, user.user_id)
