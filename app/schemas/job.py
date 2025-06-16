from pydantic import BaseModel
from datetime import datetime

class JobBase(BaseModel):
    job_id: str
    user_id: str
    status: str

class JobCreate(JobBase):
    pass

class JobUpdate(JobBase):
    pass

class JobRead(JobBase):
    created_at: datetime
