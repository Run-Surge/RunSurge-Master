from pydantic import BaseModel
from datetime import datetime
from typing import Optional             
from app.db.models.scheme import JobType, JobStatus
class JobBase(BaseModel):
    pass

class JobCreate(BaseModel):
    job_name: str
    job_type: JobType
    script_name: str

class ComplexJobCreate(BaseModel):
    user_id: int
    job_name: str
    job_type: JobType
    group_id: int
    script_path: str
class JobUpdate(JobBase):
    pass

class JobRead(JobBase):
    job_id: int
    status: JobStatus
    created_at: datetime
    job_name: str
    job_type: JobType
    script_name: Optional[str] = None

class JobDetailRead(JobRead):
    input_file_name: str

class ComplexJobRead(BaseModel):
    group_id: int
    job_id: int
    status: JobStatus

class ComplexJobDetailRead(ComplexJobRead):
    input_file_name: str
