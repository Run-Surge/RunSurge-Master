from fastapi import Depends, APIRouter
from app.schemas.user import UserCreate, UserRead
from app.services.user import UserService, get_user_service     


router = APIRouter()


    
@router.post("/", response_model=UserRead)
async def create_user(user: UserCreate, user_service: UserService = Depends(get_user_service)):
    return await user_service.create_user(user)