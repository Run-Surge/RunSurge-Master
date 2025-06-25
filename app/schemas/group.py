from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional
from app.schemas.job import ComplexJobRead

class GroupRead(BaseModel):
    group_id: int
    group_name: str
    python_file_name: str
    num_of_jobs: int
    created_at: datetime
    jobs: Optional[List[ComplexJobRead]] = []
    
    model_config = {
        "from_attributes": True
    }

