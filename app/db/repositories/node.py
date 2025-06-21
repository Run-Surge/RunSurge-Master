from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from app.db.models.scheme import Node, NodeResources
from app.db.repositories.base import BaseRepository
from typing import Optional, List
from fastapi import Depends
from app.db.session import get_db
from app.schemas.node import NodeRegisterGRPC
from sqlalchemy.ext.asyncio import AsyncSession

class NodeRepository(BaseRepository[Node]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Node)

    async def create_node(self, node: NodeRegisterGRPC) -> Node:
        print(node)
        db_node = Node(
            machine_fingerprint=node.machine_fingerprint,
            user_id=node.user_id,
            ram=node.memory_bytes,
            ip_address=node.ip_address,
            port=node.port,
        )
        return await self.create(db_node)
    
    async def get_node_by_fingerprint_user_id(self, fingerprint: str, user_id: int) -> Optional[Node]:
        statement = select(Node).where(
            and_(Node.machine_fingerprint == fingerprint, Node.user_id == user_id)
        )
        result = await self.session.execute(statement)
        return result.scalars().first()

    async def node_name_exists(self, node_name: str, user_id: int) -> bool:
        statement = select(Node).where(
            and_(Node.node_name == node_name, Node.user_id == user_id)
        )
        result = await self.session.execute(statement)
        return result.first() is not None

    async def get_by_ip_and_port(self, ip_address: str, port: int) -> Optional[Node]:
        statement = select(Node).where(
            and_(Node.ip_address == ip_address, Node.port == port)
        )
        result = await self.session.execute(statement)
        return result.scalars().first()

    async def get_available_nodes(self, required_ram: int) -> List[Node]:
        statement = select(Node).join(NodeResources).where(
            NodeResources.rem_ram >= required_ram
        )
        result = await self.session.execute(statement)
        return result.scalars().all()

    async def get_user_nodes(self, user_id: int) -> List[Node]:
        statement = select(Node).where(Node.user_id == user_id)
        result = await self.session.execute(statement)
        return result.scalars().all()
    
    async def get_all_nodes(self) -> List[Node]:
        statement = select(Node)
        result = await self.session.execute(statement)
        return result.scalars().all()

async def get_node_repository(session: AsyncSession = Depends(get_db)) -> NodeRepository:
    return NodeRepository(session)