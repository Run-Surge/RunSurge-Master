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

@router.get("/{user_id}/jobs")
def get_user_jobs(user_id: int, current_user: User = Depends(get_current_user_optional)):
    if not current_user:
        return {
            "user": None,
            "message": "User not authenticated"
        }
    if current_user.user_id != user_id:
        return {
            "user": None,
            "message": "User not authorized"
        }
    return current_user.jobs


