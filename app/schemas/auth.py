from pydantic import BaseModel, EmailStr

class UserRegisterCreate(BaseModel):
    username: str
    email: EmailStr
    password: str

class UserRegisterRead(BaseModel):
    user_id: int
    username: str
    email: EmailStr

class RefreshRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str