from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Depends, Security, Request
from fastapi.security import OAuth2PasswordBearer
from typing import Optional, Dict
from jose import JWTError, jwt
from app.core.config import settings
from app.db.models.scheme import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
oauth2_scheme_optional = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

class SecurityManager:
    def __init__(self):
        self.password_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        self.secret_key = settings.SECRET_KEY
        self.algorithm = settings.ALGORITHM
        self.oauth2_scheme = oauth2_scheme

    def hash_password(self, password: str) -> str:
        return self.password_context.hash(password)

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        return self.password_context.verify(plain_password, hashed_password)

    def create_tokens(self, user: User, access_expires_delta: Optional[timedelta] = timedelta(minutes=60), 
                     refresh_expires_delta: Optional[timedelta] = timedelta(days=7)) -> Dict[str, str]:
        data = {
            "sub": user.username,
            "user_id": user.user_id,
            "username": user.username,
            "email": user.email,
            "role": 'user' #user.role #TODO add role to user Model
        }
        access_token = self.create_access_token(data, access_expires_delta)
        refresh_token = self.create_refresh_token(data, refresh_expires_delta)
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }

    def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)) -> str:
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + expires_delta
        to_encode.update({"exp": expire, "type": "access"})
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt

    def create_refresh_token(self, data: dict, expires_delta: Optional[timedelta] = timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)) -> str:
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + expires_delta
        to_encode.update({"exp": expire, "type": "refresh"})
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt

    def verify_refresh_token(self, token: str) -> dict:
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            if payload.get("type") != "refresh":
                raise JWTError("Invalid token type")
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token has expired")
        except jwt.JWTError as e:
            print(f"JWTError: {e}")
            raise JWTError("Invalid refresh token")

    def verify_token(self, token: str):
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            if payload.get("type") != "access":
                raise HTTPException(status_code=401, detail="Invalid token")
            
            return payload  
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token has expired")
        except jwt.JWTError as e:
            print(f"JWTError: {e}")
            raise HTTPException(status_code=401, detail="Invalid token")


# Create a singleton instance
security_manager = SecurityManager()
    
def get_current_user(token: str = Depends(oauth2_scheme)):
    return security_manager.verify_token(token)

def get_current_user_optional(token: Optional[str] = Depends(oauth2_scheme_optional)):
    if token:
        return security_manager.verify_token(token)
    return None

def get_current_user_from_cookie(request: Request):
    """Get current user from cookie"""
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="Access token not found in cookies")
    return security_manager.verify_token(token)

def get_current_user_from_cookie_optional(request: Request):
    """Get current user from cookie"""
    token = request.cookies.get("access_token")
    if not token:
        return None
    try:
        return security_manager.verify_token(token)
    except HTTPException:
        return None
