from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List             
from app.db.models.scheme import JobType, JobStatus, PaymentStatus
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
    payment_amount: Optional[float] = None

class ComplexJobRead(BaseModel):
    group_id: int
    job_id: int
    status: JobStatus

class ComplexJobDetailRead(ComplexJobRead):
    input_file_name: str
    payment_amount: Optional[float] = None
    

class TaskPaymentRead(BaseModel):
    task_id: int
    total_active_time: float
    avg_memory_bytes: int
    task_payment_amount: Optional[float] = None

class PaymentRead(BaseModel):
    job_id: int
    amount: float
    status: PaymentStatus
    num_of_tasks: int
    payment_date: Optional[datetime] = None
    tasks: List[TaskPaymentRead]
    