from app.db.repositories.node import NodeRepository
from app.schemas.node import NodeRegisterGRPC, NodeUpdateGRPC
from fastapi import HTTPException  
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.logging import setup_logging
from sqlalchemy.exc import IntegrityError
import traceback
from app.db.models.scheme import Node

logger = setup_logging("NodeService")

class NodeService:
    def __init__(self, node_repo: NodeRepository):
        self.node_repo = node_repo
        self.logger = setup_logging("NodeService")

    async def create_node(self, node: NodeRegisterGRPC) -> Node:        
        logger.info(f"Creating node: {node}")
        try:
            return await self.node_repo.create_node(node)
        except IntegrityError:
            print('here1')
            self.logger.debug(f"node already exists")
            await self.node_repo.session.rollback()
            return await self.node_repo.get_node_by_fingerprint_user_id(node.machine_fingerprint, node.user_id)
        except Exception as e:
            print('here2')
            self.logger.error(f"Error creating node: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def register_node(self, node: NodeRegisterGRPC) -> Node:
        try:
            db_node = await self.create_node(node)
            #TODO: check if node is alive
            db_node.ip_address = node.ip_address
            db_node.port = node.port
            db_node.is_alive = True
            await self.node_repo.update(db_node)
            return db_node
        except Exception as e:
            print(traceback.format_exc())
            self.logger.debug(f"Error registering node: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def get_all_nodes(self):
        return await self.node_repo.get_all_nodes()

    async def get_user_nodes(self, user_id: int):
        return await self.node_repo.get_user_nodes(user_id)
    
    async def update_node_status(self, node: NodeUpdateGRPC):
        db_node = await self.node_repo.get_by_id(node.node_id)
        if not db_node:
            raise HTTPException(status_code=404, detail="Node not found")
        db_node.is_alive = node.is_alive
        return await self.node_repo.update(db_node)


def get_node_service(session: AsyncSession) -> NodeService:
    return NodeService(NodeRepository(session))