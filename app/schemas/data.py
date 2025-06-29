from pydantic import BaseModel

class DataCreate(BaseModel):
    file_name: str
    job_id: int

class DataRead(BaseModel):
    data_id: int
    file_name: str
    job_id: int