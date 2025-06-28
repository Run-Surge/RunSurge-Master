from fastapi import Depends, APIRouter, HTTPException
from app.core.security import get_current_user_from_cookie_optional, get_current_user_from_cookie
from app.db.models.scheme import User, TaskStatus
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.job import get_job_service
from app.services.node import get_node_service
from app.schemas.job import JobRead
from app.schemas.group import GroupRead
from app.schemas.node import NodeRead, DashboardRead
from typing import List
from app.services.group import get_group_service
from app.services.earnings import get_earnings_service
import traceback
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
    jobs = await job_service.get_simple_user_jobs(current_user["user_id"])
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

@router.get("/nodes",response_model=DashboardRead)
async def get_user_nodes(
    current_user = Depends(get_current_user_from_cookie),
    session: AsyncSession = Depends(get_db)
):
    """Get all nodes registered by the current user."""
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Authentication required")
        node_service = get_node_service(session)
        earnings_service = get_earnings_service(session)
        nodes = await node_service.get_user_nodes(current_user["user_id"])
        total_earnings = await node_service.get_total_earnings(nodes)
        paid_earnings = await earnings_service.get_paid_earnings(nodes)
        pending_earnings = await earnings_service.get_pending_earnings(nodes)
        print("Here")
        return DashboardRead(
                total_earnings=total_earnings,
                paid_earnings=paid_earnings,
                pending_earnings=pending_earnings,
                number_of_nodes=len(nodes),
                nodes=[
            NodeRead(
                node_id=node.node_id,
                created_at=node.created_at,
                user_id=node.user_id,
                is_alive=node.is_alive,
                total_node_earnings=sum(earning.amount for earning in node.earnings),
                num_of_completed_tasks=sum(1 for task in node.tasks if task.status == TaskStatus.completed),
            ) for node in nodes
            ]
        )
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to fetch nodes")

@router.get("/groups", response_model=List[GroupRead])
async def get_groups(
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        group_service = get_group_service(session)
        groups = await group_service.get_groups_by_user_id(user_id=current_user["user_id"])
        return groups
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


