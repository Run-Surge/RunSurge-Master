from pydantic import BaseModel, Field
from datetime import datetime

class UserBase(BaseModel):
    user_id: int
    username: str
    email: str

class UserLoginCreate(BaseModel):
    username_or_email: str
    password: str

class UserLoginRead(BaseModel):
    user_id: int
    username: str
    email: str


class UserUpdate(UserBase):
    pass
