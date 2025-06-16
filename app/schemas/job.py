from pydantic import BaseModel
from datetime import datetime

class JobBase(BaseModel):
    job_id: int
    user_id: int
    status: str

class JobCreate(JobBase):
    pass

class JobUpdate(JobBase):
    pass

class JobRead(JobBase):
    created_at: datetime
