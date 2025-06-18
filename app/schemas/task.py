from pydantic import BaseModel
from datetime import datetime
from typing import Optional
from app.db.models.scheme import TaskStatus

class TaskBase(BaseModel):
    job_id: int
    data_id: int
    required_ram: int

class TaskCreate(TaskBase):
    pass

class TaskRead(TaskBase):
    task_id: int
    created_at: datetime
    updated_at: datetime

class TaskUpdate(TaskBase):
    task_id: int
    node_id: Optional[int]
    data_id: Optional[int]
    status: Optional[TaskStatus]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
