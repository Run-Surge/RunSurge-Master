from fastapi import APIRouter, Depends, Form, UploadFile, File, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.core.security import get_current_user_from_cookie
from app.schemas.group import GroupRead
from app.services.group import get_group_service
from typing import List

router = APIRouter()

@router.post("/",response_model=GroupRead)
async def create_group(
    group_name: str = Form(...),
    num_of_jobs: int = Form(...),
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        group_service = get_group_service(session)
        group = await group_service.create_group(
            group_name=group_name,
            file=file,
            num_of_jobs=num_of_jobs,
            user_id=current_user["user_id"]
            )
        return group
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/", response_model=List[GroupRead])
async def get_groups(
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        group_service = get_group_service(session)
        groups = await group_service.get_groups_by_user_id(user_id=current_user["user_id"])
        return groups
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/{group_id}", response_model=GroupRead)
async def get_group_by_id(
    group_id: int,
    session: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user_from_cookie)
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        group_service = get_group_service(session)
        group = await group_service.get_group_by_id(group_id=group_id, user_id=current_user["user_id"])
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")
        return group
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

