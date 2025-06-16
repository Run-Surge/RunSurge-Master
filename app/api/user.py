from fastapi import Depends, APIRouter
from app.core.security import get_current_user_optional
from app.db.models.scheme import User

router = APIRouter()


@router.get("/me")
def get_current_user(current_user: User = Depends(get_current_user_optional)):
    if not current_user:
        return {
            "user": None,
            "message": "User not found"
        }
    return current_user
