from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class NodeBase(BaseModel):
    node_name: str
    user_id: str
    ram: int
    cpu_cores: int
    ip_address: Optional[str] = None
    port: Optional[int] = None

class NodeRead(NodeBase):
    created_at: datetime

class NodeCreate(NodeBase):
    pass

class NodeUpdate(NodeBase):
    pass