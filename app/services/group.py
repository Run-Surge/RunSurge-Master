import uuid
from app.db.repositories.group import GroupRepository
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import UploadFile, HTTPException
from app.utils.utils import validate_file, save_file, Create_directory
from app.utils.constants import GROUPS_DIRECTORY_PATH
class GroupService:
    def __init__(self, group_repo: GroupRepository):
        self.group_repo = group_repo

    async def create_group(self, group_name: str, file: UploadFile, num_of_jobs: int, user_id: int):
        try:
            validate_file(file)
            file_name = file.filename.split(".")[0]
            group = await self.group_repo.create_group(group_name, file_name, num_of_jobs, user_id)
            Create_directory(f"{GROUPS_DIRECTORY_PATH}/{group.group_id}")
            save_file(file, f"{GROUPS_DIRECTORY_PATH}/{group.group_id}/{group.group_id}.py")
            return group
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_groups_by_user_id(self, user_id: int):
        return await self.group_repo.get_groups_by_user_id(user_id)

    async def get_group_by_id(self, group_id: int, user_id: int):
        return await self.group_repo.get_group_by_id(group_id, user_id)

def get_group_service(session: AsyncSession) -> GroupService:
    return GroupService(GroupRepository(session))
