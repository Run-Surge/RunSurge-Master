from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional
from app.utils.constants import DEFAULT_PORT

class NodeBase(BaseModel):
    node_name: str
    ram: int
    ip_address: str
    port: Optional[int] = DEFAULT_PORT

class NodeRead(NodeBase):
    node_id: int
    created_at: datetime
    user_id: int
    model_config = ConfigDict(from_attributes=True)

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