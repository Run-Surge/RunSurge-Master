from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional
from app.utils.constants import DEFAULT_PORT
from app.db.models.scheme import EarningStatus, TaskStatus
from typing import List

class NodeBase(BaseModel):
    node_name: str
    ram: int
    ip_address: str
    port: Optional[int] = DEFAULT_PORT



class NodeRead(BaseModel):
    node_id: int
    created_at: datetime
    user_id: int
    is_alive: bool
    total_node_earnings: float
    num_of_completed_tasks: int


class DashboardRead(BaseModel):
    total_earnings: float
    paid_earnings: float
    pending_earnings: float
    number_of_nodes: int
    nodes: List[NodeRead]


class NodeCreate(NodeBase):
    machine_fingerprint: str
    is_alive: bool

class NodeUpdate(NodeBase):
    pass


class NodeRegisterGRPC(BaseModel):
    user_id: int
    machine_fingerprint: str
    memory_bytes: int
    ip_address: str
    port: int

class NodeUpdateGRPC(BaseModel):
    node_id: int
    is_alive: bool

class TaskNodeDetailRead(BaseModel):
    task_id: int
    started_at: datetime
    completed_at: datetime
    total_active_time: float
    avg_memory_bytes: int
    status: TaskStatus
    earning_amount: float
    earning_status: EarningStatus

class NodeDetailRead(BaseModel):
    node_id: int
    is_alive: bool
    total_node_earnings: float
    num_of_completed_tasks: int
    tasks: List[TaskNodeDetailRead]
