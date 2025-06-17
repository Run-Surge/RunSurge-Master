from fastapi import Depends, HTTPException, APIRouter, Form
from app.db.repositories.job import JobRepository, get_job_repository
from app.schemas.job import JobRead, JobCreate
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.job import get_job_service
from typing import Optional
from fastapi import UploadFile, File
from app.core.security import get_current_user
from app.db.models.scheme import User
import logging
router = APIRouter()

@router.post("/", response_model=JobRead)
async def create_job(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db), 
    current_user: User = Depends(get_current_user)
):
    logging.info(f"HEREERRERERE: {file}")  
    job_service = get_job_service(session)
    return await job_service.create_job_with_script(
        user_id=current_user["user_id"],
        file=file,
    )

