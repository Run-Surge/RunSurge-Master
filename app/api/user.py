from fastapi import Depends, APIRouter, HTTPException
from app.core.security import get_current_user_from_cookie_optional, get_current_user_from_cookie
from app.db.models.scheme import User
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.job import get_job_service
from app.services.node import get_node_service
from app.schemas.job import JobRead
router = APIRouter()


@router.get("/me")
def get_current_user(current_user = Depends(get_current_user_from_cookie_optional)):
    if not current_user:
        return {
            "status": "unauthenticated",
            "user": None,
            "message": "User not found or not logged in."
        }
    return {
        "status": "authenticated",
        "user": current_user,
        "message": "User fetched successfully."
    }


@router.get("/jobs")
async def get_user_jobs(
    current_user = Depends(get_current_user_from_cookie),
    session: AsyncSession = Depends(get_db)
):
    """Get all jobs submitted by the current user."""
    if not current_user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    job_service = get_job_service(session)
    jobs = await job_service.get_user_jobs(current_user["user_id"])
    return {
        "jobs": [JobRead(
            job_id=job.job_id,
            job_name=job.job_name,
            job_type=job.job_type,
            status=job.status,
            created_at=job.created_at,
            user_id=current_user["user_id"],
            script_name=job.script_name,
        ) for job in jobs],
        "message": "Jobs fetched successfully",
        "success": True
    }

@router.get("/nodes")
async def get_user_nodes(
    current_user = Depends(get_current_user_from_cookie),
    session: AsyncSession = Depends(get_db)
):
    """Get all nodes registered by the current user."""
    if not current_user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    node_service = get_node_service(session)
    nodes = await node_service.get_user_nodes(current_user["user_id"])
    return {
        "nodes": nodes,
        "message": "Nodes fetched successfully",
        "success": True
    }




