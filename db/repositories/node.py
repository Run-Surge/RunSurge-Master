from sqlmodel import Session, select
from db.models.scheme import Node, NodeResources
from db.repositories.base import BaseRepository
from typing import Optional, List
from datetime import datetime
import uuid

class NodeRepository(BaseRepository[Node]):
    def __init__(self, session: Session):
        super().__init__(session, Node)

    def create_node(self, username: str, ram: int, cpu_cores: int, 
                   ip_address: str, port: int) -> Node:
        node = Node(
            username=username,
            ram=ram,
            cpu_cores=cpu_cores,
            ip_address=ip_address,
            port=port
        )
        return self.create(node)

    def get_by_ip_and_port(self, ip_address: str, port: int) -> Optional[Node]:
        statement = select(Node).where(
            (Node.ip_address == ip_address) & (Node.port == port)
        )
        return self.session.exec(statement).first()

    def get_available_nodes(self, required_ram: int) -> List[Node]:
        statement = select(Node).join(NodeResources).where(
            NodeResources.rem_ram >= required_ram
        )
        return self.session.exec(statement).all()