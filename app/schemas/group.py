from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional
from app.schemas.job import ComplexJobRead, ComplexJobDetailRead
from app.db.models.scheme import GroupStatus, PaymentStatus

class GroupBase(BaseModel):
    group_id: int
    group_name: str
    python_file_name: str
    num_of_jobs: int
    created_at: datetime
    aggregator_file_name: str
    status: GroupStatus
    payment_status: Optional[PaymentStatus] = PaymentStatus.pending
    payment_amount: Optional[float] = None
class GroupRead(GroupBase):
    jobs: Optional[List[ComplexJobRead]] = []

class GroupDetailRead(GroupBase):
    jobs: List[ComplexJobDetailRead]
    


