import uuid
from app.db.repositories.group import GroupRepository
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import UploadFile, HTTPException
from app.utils.utils import validate_file, save_file, Create_directory
from app.utils.constants import GROUPS_DIRECTORY_PATH
from app.db.models.scheme import JobStatus, GroupStatus
import logging
import os
import zipfile
import traceback
from app.services.task import get_task_service  
from typing import List
from app.db.models.scheme import Task
from datetime import datetime
from app.db.models.scheme import PaymentStatus, EarningStatus, TaskStatus

class GroupService:
    def __init__(self, group_repo: GroupRepository):
        self.group_repo = group_repo
        self.logger = logging.getLogger(__name__)

    async def create_group(self, group_name: str, python_file: UploadFile, num_of_jobs: int, user_id: int, aggregator_file: UploadFile):
        try:
            validate_file(python_file)
            validate_file(aggregator_file)
            python_file_name = python_file.filename.split(".")[0]
            aggregator_file_name = aggregator_file.filename.split(".")[0]
            random_name = str(uuid.uuid4())
            group = await self.group_repo.create_group(group_name, python_file_name, aggregator_file_name,random_name, num_of_jobs, user_id)
            Create_directory(f"{GROUPS_DIRECTORY_PATH}/{group.group_id}")
            python_path = f"{GROUPS_DIRECTORY_PATH}/{group.group_id}/{group.group_id}.py"
            save_file(python_file, python_path)
            aggregator_path = f"{GROUPS_DIRECTORY_PATH}/{group.group_id}/{random_name}.py"
            save_file(aggregator_file, aggregator_path) 
            return group, python_path
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

    async def get_group_tasks(self, group_id: int) -> List[Task]:
        group = await self.group_repo.get_group_by_id(group_id)
        tasks = []
        for job in group.jobs:
            tasks.extend(await get_task_service(self.group_repo.session).get_tasks_with_job_id(job.job_id))
        return tasks
    
    async def pay_group(self, group_id: int) -> bool:
        try:
            jobs = await self.group_repo.get_group_jobs(group_id)
            tasks = await self.get_group_tasks(group_id)
            for task in tasks:
                task_earning = task.earning
                task_earning.status = EarningStatus.paid
                task_earning.earning_date = datetime.now()
            for job in jobs:
                job.payment.status = PaymentStatus.completed
                job.payment.payment_date = datetime.now()
            
            # Update group payment status
            group = await self.group_repo.get_group_by_id(group_id)
            group.payment_status = PaymentStatus.completed
            group.payment_date = datetime.now()
            await self.group_repo.update(group)
            return True
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Error paying group {group_id}: {e}")        
    
    async def update_group_after_job_completion(self, group_id: int) -> bool:
        try:
            self.logger.info(f"Updating group {group_id} after job completion")
            group = await self.group_repo.get_group_with_jobs(group_id)
            is_all_jobs_completed = all(job.status == JobStatus.completed for job in group.jobs)
        
            if not is_all_jobs_completed:
                return False
            
            
            self.logger.info(f"Group {group_id} is all jobs completed, will start aggregating")
            group_dir = f"{GROUPS_DIRECTORY_PATH}/{group_id}"
            output_zip = zipfile.ZipFile(f"{group_dir}/group_output.zip", 'w')

            total_cost = 0

            for job in group.jobs:
                output_file_path = f"{group_dir}/output_{job.job_id}.zip"
                if not os.path.exists(output_file_path):
                    raise HTTPException(status_code=404, detail=f"Output file {output_file_path} not found, while aggregating group {group_id}")
                job_tasks = await get_task_service(self.group_repo.session).get_tasks_with_job_id(job.job_id)
                for task in job_tasks:
                    if task.status == TaskStatus.completed:
                        print("I am here", task.earning.amount)
                        total_cost += task.earning.amount

                # Add the job's output zip file to the group output zip
                output_zip.write(output_file_path, os.path.basename(output_file_path))
                self.logger.info(f"Added {output_file_path} to group output zip")

            output_zip.close()
            self.logger.info(f"Group {group_id} output zip created")

            group.payment_amount = total_cost
            print(f"Group {group_id} payment amount: {group.payment_amount}")
            await self.update_group_status(group_id, GroupStatus.completed)
            self.logger.info(f"Group {group_id} status updated to completed")
            return True
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Error updating group {group_id} after job completion: {e}")
            raise HTTPException(status_code=500, detail=str(e))

def get_group_service(session: AsyncSession) -> GroupService:
    return GroupService(GroupRepository(session))
