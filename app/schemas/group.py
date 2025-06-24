from pydantic import BaseModel
from datetime import datetime

class GroupRead(BaseModel):
    group_id: int
    group_name: str
    python_file_name: str
    num_of_jobs: int
    created_at: datetime

