from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from app.db.models.scheme import Node, NodeResources
from app.db.repositories.base import BaseRepository
from typing import Optional, List
from fastapi import Depends
from app.db.session import get_db
from app.schemas.node import NodeCreate

class NodeRepository(BaseRepository[Node]):
    def __init__(self, session: Session):
        super().__init__(session, Node)

    async def create_node(self, node: NodeCreate) -> Node:
        db_node = Node(
            node_name=node.node_name,
            user_id=node.user_id,
            ram=node.ram,
            cpu_cores=node.cpu_cores,
            ip_address=node.ip_address,
            port=node.port
        )
        await self.create(db_node)
        return db_node

    async def node_name_exists(self, node_name: str, user_id: str) -> bool:
        statement = select(Node).where(
            and_(Node.node_name == node_name, Node.user_id == user_id)
        )
        result = self.session.execute(statement)
        return result.scalar_one_or_none() is not None

    async def get_by_ip_and_port(self, ip_address: str, port: int) -> Optional[Node]:
        statement = select(Node).where(
            and_(Node.ip_address == ip_address, Node.port == port)
        )
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()

    async def get_available_nodes(self, required_ram: int) -> List[Node]:
        statement = select(Node).join(NodeResources).where(
            NodeResources.rem_ram >= required_ram
        )
        result = await self.session.execute(statement)
        return result.scalars().all()

def get_node_repository(session: Session = Depends(get_db)) -> NodeRepository:
    return NodeRepository(session)