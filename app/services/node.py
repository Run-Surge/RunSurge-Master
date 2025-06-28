from app.db.repositories.node import NodeRepository
from app.schemas.node import NodeRegisterGRPC, NodeUpdateGRPC
from fastapi import HTTPException  
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.logging import setup_logging
from sqlalchemy.exc import IntegrityError
import traceback
from app.db.models.scheme import Node, NodeLog, TaskStatus
from app.db.repositories.node_log import NodeLogRepository
from protos import master_pb2
from datetime import datetime
from typing import List

logger = setup_logging("NodeService")

class NodeService:
    def __init__(self, node_repo: NodeRepository, node_log_repo: NodeLogRepository):
        self.node_repo = node_repo
        self.node_log_repo = node_log_repo
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
            session = self.node_log_repo.session
            #TODO: check if node is alive
            db_node.ram = node.memory_bytes
            db_node.ip_address = node.ip_address
            db_node.port = node.port
            db_node.is_alive = True
            db_node.last_heartbeat = datetime.now()
            
            session.add(NodeLog(
                node_id=db_node.node_id,
                number_of_tasks=0,
                memory_usage_bytes=0
            ))

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
    
    async def get_total_earnings(self, nodes: List[Node]):
        return sum(sum(earning.amount for earning in node.earnings) for node in nodes)
    async def get_num_of_completed_tasks(self, nodes: List[Node]):
        return sum(sum(1 for task in node.tasks if task.status == TaskStatus.completed) for node in nodes)

    async def update_node_heartbeat(self, node: master_pb2.NodeHeartbeatRequest):
        db_node = await self.node_repo.get_by_id(node.node_id)
        
        if not db_node:
            raise HTTPException(status_code=404, detail="Node not found")
        
        db_node.is_alive = True
        db_node.last_heartbeat = datetime.now()

        await self.node_repo.update(db_node)
        await self.node_log_repo.create(NodeLog(
            node_id=node.node_id,
            number_of_tasks=node.number_of_tasks,
            memory_usage_bytes=node.memory_usage_bytes
        ))

    async def update_dead_nodes(self):
        current_time = datetime.now()
        nodes = await self.node_repo.get_defered_nodes()
        session = self.node_repo.session
        self.logger.debug(f"Found {len(nodes)} dead nodes")
        for node in nodes:
            print(f"Updating node {node.node_id} to dead, last heartbeat: {node.last_heartbeat}, current time: {current_time}")
            node.is_alive = False
            await self.node_repo.update(node)
            session.add(NodeLog(
                node_id=node.node_id,
                number_of_tasks=0,
                memory_usage_bytes=0
            ))
            
    async def get_node(self, node_id: int):
        return await self.node_repo.get_by_id(node_id)
      
    async def 
    
    _joined_tasks_earnings(self, node_id: int):
        return await self.node_repo.get_node_joined_tasks_earnings(node_id)


        await session.commit()

def get_node_service(session: AsyncSession) -> NodeService:
    return NodeService(NodeRepository(session), NodeLogRepository(session)  )