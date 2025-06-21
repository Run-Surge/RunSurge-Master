from app.db.repositories.node import NodeRepository
from app.schemas.node import NodeCreate
from fastapi import HTTPException  
from sqlalchemy.ext.asyncio import AsyncSession

class NodeService:
    def __init__(self, node_repo: NodeRepository):
        self.node_repo = node_repo

    async def create_node(self, node: NodeCreate, user_id: int):
        if await self.node_repo.node_name_exists(node.node_name, user_id):
            raise HTTPException(status_code=400, detail="Node name already exists")
        return await self.node_repo.create_node(node, user_id)

    async def get_all_nodes(self):
        return await self.node_repo.get_all_nodes()

    async def get_user_nodes(self, user_id: int):
        return await self.node_repo.get_user_nodes(user_id)

def get_node_service(session: AsyncSession) -> NodeService:
    return NodeService(NodeRepository(session))