from pydantic import BaseModel
from datetime import datetime
from typing import Optional             

class JobBase(BaseModel):
    job_id: int
    status: str

class JobCreate(BaseModel):
    user_id: int

class JobUpdate(JobBase):
    pass

class JobRead(JobBase):
    created_at: datetime

