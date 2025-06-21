from fastapi import Depends, APIRouter, HTTPException
from app.core.security import get_current_user_optional
from app.db.models.scheme import User
from app.schemas.job import JobRead
from app.schemas.node import NodeRead
from typing import List
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.job import get_job_service
from app.services.node import get_node_service

router = APIRouter()


@router.get("/me")
def get_current_user(current_user: User = Depends(get_current_user_optional)):
    if not current_user:
        return {
            "user": None,
            "message": "User not found"
        }
    return current_user


@router.get("/jobs", response_model=List[JobRead])
async def get_user_jobs(
    current_user: User = Depends(get_current_user_optional),
    session: AsyncSession = Depends(get_db)
):
    """Get all jobs submitted by the current user."""
    print("current_user",current_user)
    if not current_user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    job_service = get_job_service(session)
    jobs = await job_service.get_user_jobs(current_user["user_id"])
    return jobs

@router.get("/nodes", response_model=List[NodeRead])
async def get_user_nodes(
    current_user: User = Depends(get_current_user_optional),
    session: AsyncSession = Depends(get_db)
):
    """Get all nodes registered by the current user."""
    print("current_user",current_user)
    if not current_user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    node_service = get_node_service(session)
    return await node_service.get_user_nodes(current_user["user_id"])




