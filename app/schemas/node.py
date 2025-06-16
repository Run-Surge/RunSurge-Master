from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional

class NodeBase(BaseModel):
    node_name: str
    ram: int
    cpu_cores: int
    ip_address: Optional[str] = None
    port: Optional[int] = None

class NodeRead(NodeBase):
    node_id: int
    created_at: datetime
    user_id: int
    model_config = ConfigDict(from_attributes=True)

class NodeCreate(NodeBase):
    pass

class NodeUpdate(NodeBase):
    pass