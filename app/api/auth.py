from fastapi import APIRouter, Depends, HTTPException, status
from app.core.security import security_manager
from app.schemas.auth import UserRegister, TokenResponse, RefreshRequest
from app.schemas.user import UserLogin
from app.services.user import get_user_service
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

@router.post("/register", response_model=TokenResponse)
async def register(user: UserRegister, session: AsyncSession = Depends(get_db)):
    user_service = get_user_service(session)
    if await user_service.user_exists(user.username, user.email):
        raise HTTPException(status_code=400, detail="Username or email already exists")
    
    db_user = await user_service.create_user(user)
    
    # Create tokens using the unified function
    return security_manager.create_tokens(user=db_user)

@router.post("/login", response_model=TokenResponse)
async def login(user: UserLogin, session: AsyncSession = Depends(get_db)):
    user_service = get_user_service(session)
    db_user = await user_service.login_user(user)
    
    return security_manager.create_tokens(user=db_user)

@router.post("/refresh", response_model=TokenResponse)
async def refresh(refresh_token: RefreshRequest, session: AsyncSession = Depends(get_db)):
    user_service = get_user_service(session)
    try:
        print(f"refresh_token: {refresh_token.refresh_token}")
        payload = security_manager.verify_refresh_token(refresh_token.refresh_token)
        
        user_id = payload.get("user_id")
        print(f"user_id: {user_id}")
        user = await user_service.get_user_by_id(user_id)
        print(f"hello world: {user}")
        if not user:
            raise HTTPException(status_code=400, detail="User not found")
        
        return security_manager.create_tokens(user=user)
    except Exception as e:
        print(f"Exception: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )