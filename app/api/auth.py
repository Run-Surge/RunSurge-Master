from fastapi import APIRouter, Depends, HTTPException, status, Response
from app.core.security import security_manager
from app.schemas.auth import UserRegisterCreate, UserRegisterRead, TokenResponse, RefreshRequest
from app.schemas.user import UserLoginCreate, UserLoginRead
from app.services.user import get_user_service
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
# TODO: modify this not to reutrn the password in the response

@router.post("/register")
async def register(user: UserRegisterCreate, response: Response, session: AsyncSession = Depends(get_db)):
    user_service = get_user_service(session)
    if await user_service.user_exists(user.username, user.email):
        raise HTTPException(status_code=400, detail="Username or email already exists")
    
    db_user = await user_service.create_user(user)
    
    tokens = security_manager.create_tokens(user=db_user)
    
    response.set_cookie(
        key="access_token",
        value=tokens["access_token"],
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=3600
    )
    
    response.set_cookie(
        key="refresh_token", 
        value=tokens["refresh_token"],
        httponly=True,
        secure=False, 
        samesite="lax",
        max_age=604800  
    )
    
    return {
        "user": db_user,
        "message": "User registered successfully",
        "success": True
    }

@router.post("/login")
async def login(user: UserLoginCreate, response: Response, session: AsyncSession = Depends(get_db)):
    user_service = get_user_service(session)
    db_user = await user_service.login_user(user)
    
    tokens = security_manager.create_tokens(user=db_user)
    
    response.set_cookie(
        key="access_token",
        value=tokens["access_token"],
        httponly=True,
        secure=False,       
        samesite="lax",
        max_age=3600
    )
    
    response.set_cookie(
        key="refresh_token",
        value=tokens["refresh_token"],
        httponly=True,
        secure=False,   
        samesite="lax",
        max_age=604800
    )
    
    return {
        "user": db_user,
        "message": "User logged in successfully",
        "success": True
    }

@router.post("/refresh")
async def refresh(refresh_token: RefreshRequest, response: Response, session: AsyncSession = Depends(get_db)):
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
        
        tokens = security_manager.create_tokens(user=user)
        
        response.set_cookie(
            key="access_token",
            value=tokens["access_token"],
            httponly=True,
            secure=False,   
            samesite="lax",
            max_age=3600
        )
        
        response.set_cookie(
            key="refresh_token",
            value=tokens["refresh_token"],
            httponly=True,
            secure=False,   
            samesite="lax",
            max_age=604800
        )
        
        return tokens
    except Exception as e:
        print(f"Exception: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )

@router.post("/logout")
async def logout(response: Response):
    response.delete_cookie(key="access_token")
    response.delete_cookie(key="refresh_token")
    return {"message": "Successfully logged out"}