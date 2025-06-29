import uuid
from app.db.repositories.group import GroupRepository
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import UploadFile, HTTPException
from app.utils.utils import validate_file, save_file, Create_directory, validate_aggregator_file
from app.utils.constants import GROUPS_DIRECTORY_PATH
from app.db.models.scheme import JobStatus, GroupStatus
import logging
import os
import zipfile
import traceback
from app.core.logging import setup_logging

class GroupService:
    def __init__(self, group_repo: GroupRepository):
        self.group_repo = group_repo
        self.logger = setup_logging(__name__)

    async def create_group(self, group_name: str, python_file: UploadFile, num_of_jobs: int, user_id: int, aggregator_file: UploadFile):
        try:
            validate_file(python_file)
            validate_file(aggregator_file)
            before_code, loop_code, after_code = validate_aggregator_file(aggregator_file)
            python_file_name = python_file.filename.split(".")[0]
            aggregator_file_name = aggregator_file.filename.split(".")[0]
            random_name = f"{str(uuid.uuid4())}.py"
            group = await self.group_repo.create_group(group_name, python_file_name, aggregator_file_name,random_name, num_of_jobs, user_id)
            Create_directory(f"{GROUPS_DIRECTORY_PATH}/{group.group_id}")
            python_path = f"{GROUPS_DIRECTORY_PATH}/{group.group_id}/{group.group_id}.py"
            save_file(python_file, python_path)
            aggregator_path = f"{GROUPS_DIRECTORY_PATH}/{group.group_id}/{random_name}"
            return group, python_path, aggregator_path, before_code, loop_code, after_code
        except HTTPException as e:
            raise e
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_groups_by_user_id(self, user_id: int):
        return await self.group_repo.get_groups_by_user_id(user_id)

    async def get_group_by_id(self, group_id: int):
        return await self.group_repo.get_group_by_id(group_id)
    
    async def update_group_status(self, group_id: int, status: GroupStatus):
        group = await self.group_repo.get_group_by_id(group_id)
        group.status = status
        await self.group_repo.update(group)
    
    async def update_group_after_job_completion(self, group_id: int) -> bool:
        try:
            self.logger.info(f"Updating group {group_id} after job completion")
            group = await self.group_repo.get_group_with_jobs(group_id)
            is_all_jobs_completed = all(job.status == JobStatus.completed for job in group.jobs)
        
            if not is_all_jobs_completed:
                return False
            
            self.logger.info(f"Group {group_id} is all jobs completed, will start aggregating")
            await self.update_group_status(group_id, GroupStatus.pending_aggregation)
            self.logger.info(f"Group {group_id} status updated to pending_aggregation")
            return True
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Error updating group {group_id} after job completion: {e}")
            raise HTTPException(status_code=500, detail=str(e))


def get_group_service(session: AsyncSession) -> GroupService:
    return GroupService(GroupRepository(session))
