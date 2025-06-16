from fastapi import Depends, APIRouter
from app.schemas.node import NodeCreate, NodeRead
from app.services.node import NodeService, get_node_service

router = APIRouter()

@router.post("/", response_model=NodeRead)
async def register_node(
    node: NodeCreate, 
    node_service: NodeService = Depends(get_node_service)
):
    created_node = await node_service.create_node(node)
    return created_node