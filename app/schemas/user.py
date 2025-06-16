from pydantic import BaseModel
from datetime import datetime

class UserBase(BaseModel):
    user_id: str
    username: str
    email: str

class UserCreate(BaseModel):
    username: str
    email: str
    password: str

class UserRead(UserBase):
    created_at: datetime

class UserUpdate(UserBase):
    pass