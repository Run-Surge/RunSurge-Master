from fastapi import Depends, APIRouter, HTTPException
from app.schemas.node import NodeCreate, NodeRead, NodeDetailRead, TaskNodeDetailRead
from app.services.node import get_node_service
from app.services.earnings import get_earnings_service
from app.core.security import  get_current_user_from_cookie
from app.db.models.scheme import User, TaskStatus   
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
import traceback
router = APIRouter()

@router.post("/", response_model=NodeRead)
async def register_node(
    node: NodeCreate, 
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user_from_cookie)
):
    node_service = get_node_service(session)
    created_node = await node_service.create_node(node, current_user["user_id"])
    return created_node

@router.get("/{node_id}", response_model=NodeDetailRead)
async def get_node(
    node_id: int,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user_from_cookie)
):
    try:
            if not current_user:
                raise HTTPException(status_code=401, detail="Authentication required")
            node_service = get_node_service(session)
            node = await node_service.get_node_joined_tasks_earnings(node_id)
            earnings_service = get_earnings_service(session)
            if node.user_id != current_user["user_id"]:
                raise HTTPException(status_code=403, detail="Forbidden")
            total_node_earnings = await node_service.get_total_earnings([node])
            num_of_completed_tasks = await node_service.get_num_of_completed_tasks([node])
            print("MANGA")
            return NodeDetailRead(
                node_id=node.node_id,
                is_alive=node.is_alive,
                total_node_earnings=total_node_earnings,
                num_of_completed_tasks=num_of_completed_tasks,
                tasks=[
                    TaskNodeDetailRead(
                        task_id=task.task_id,
                        started_at=task.started_at,
                        completed_at=task.completed_at,
                        total_active_time=task.total_active_time,
                        avg_memory_bytes=task.avg_memory_bytes,
                        status=task.status,
                        earning_amount=await earnings_service.get_earning_amount_by_task_id(task.task_id),
                        earning_status=await earnings_service.get_earning_status_by_task_id(task.task_id)
                    ) 
                    for task in node.tasks
                    if task.status == TaskStatus.completed
                ]
            )
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to fetch node")
