from fastapi import Depends, APIRouter
from app.schemas.node import NodeCreate, NodeRead
from app.services.node import NodeService, get_node_service
from app.core.security import get_current_user
from app.db.models.scheme import User
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

@router.post("/", response_model=NodeRead)
async def register_node(
    node: NodeCreate, 
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    #TODO validate node name isn't a duplicate for the user (currently done in create_node which is wrong)
    node_service = get_node_service(session)
    created_node = await node_service.create_node(node, current_user["user_id"])
    return created_node