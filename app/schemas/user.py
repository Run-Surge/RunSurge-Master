from pydantic import BaseModel
from datetime import datetime

class UserBase(BaseModel):
    user_id: int
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

class UserLogin(BaseModel):
    username_or_email: str
    password: str