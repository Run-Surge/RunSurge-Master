from app.db.repositories.node import NodeRepository
from app.schemas.node import NodeCreate
from fastapi import Depends, HTTPException  
from app.db.repositories.node import get_node_repository

class NodeService:
    def __init__(self, node_repo: NodeRepository):
        self.node_repo = node_repo

    async def create_node(self, node: NodeCreate):
        if await self.node_repo.node_name_exists(node.node_name, node.user_id):
            raise HTTPException(status_code=400, detail="Node name already exists")
        
        return await self.node_repo.create_node(node)

def get_node_service(node_repo: NodeRepository = Depends(get_node_repository)) -> NodeService:
    return NodeService(node_repo)