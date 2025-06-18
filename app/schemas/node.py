from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional
from app.utils.constants import DEFAULT_PORT

class NodeBase(BaseModel):
    node_name: str
    ram: int
    cpu_cores: int
    ip_address: str
    port: Optional[int] = DEFAULT_PORT

class NodeRead(NodeBase):
    node_id: int
    created_at: datetime
    user_id: int
    model_config = ConfigDict(from_attributes=True)

class NodeCreate(NodeBase):
    pass

class NodeUpdate(NodeBase):
    pass