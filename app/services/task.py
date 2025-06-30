from app.db.repositories.task import TaskRepository
from app.schemas.task import TaskCreate, TaskUpdate, TaskStatus, TaskDataWithNodeInfo, TaskOutputDependentInfo
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.scheme import Task, Data, DataStatus, Node, JobStatus, Earning, EarningStatus
from typing import List
from protos import master_pb2, worker_pb2
from app.services.worker_client import WorkerClient
import traceback
from app.db.repositories.earnings import EarningsRepository
from app.utils.constants import PAYMENT_FACTOR
from app.core.logging import setup_logging
from datetime import datetime
### Tasks are created internally, not using endpoints

class TaskService:
    def __init__(self, task_repo: TaskRepository, earnings_repo: EarningsRepository):
        self.task_repo = task_repo
        self.earnings_repo = earnings_repo
        self.logger = setup_logging(__name__)    

    async def create_task(self, job_id: int, data_ids: list[int], required_ram: int, node_id: int) -> Task:
        ## we may add validation here, or return true/false
        return await self.task_repo.create_task(job_id, data_ids, required_ram, node_id)

    ## This function is not used for endpoints, it is used for internal use only
    ## it should return False if the task is not found or if the task is already assigned to a node
    ## it should return True if the task is assigned to the given node
    async def assign_task_to_node(self, task_id: int, node_id: int) -> Task:
        task = await self.task_repo.get_by_id(task_id)
        if not task:
            self.logger.error(f"Task not found: {task_id}")
            return False
        if task.node_id is not None:
            self.logger.error(f"Task already assigned to node: {task.node_id}")
            return False
        task.node_id = node_id
        await self.task_repo.update(task)
        return True

    async def get_task_output_dependencies_with_node_info(self, task_id: int) -> List[TaskOutputDependentInfo]:
        """
        Get all output files from a task and the tasks that depend on them with node information.
        
        This query:
        1. Finds all Data records where parent_task_id = task_id (output files)
        2. Finds all Tasks that depend on those data files (through TaskDataDependency)
        3. Gets the Node information for those dependent tasks
        
        Returns output files with their dependent tasks and node information.
        """
        return await self.task_repo.get_task_dependents_info(task_id)
    
    async def _calculate_earning(self, completion_info: master_pb2.TaskCompleteRequest, task: Task):
        print(f"Memory usage total: {completion_info.average_memory_bytes}, Total time elapsed: {completion_info.total_time_elapsed}, Payment factor: {task.node.payment_factor}, Payment factor: {PAYMENT_FACTOR}")
        earnings = completion_info.average_memory_bytes * \
             completion_info.total_time_elapsed * \
             task.node.payment_factor * \
             PAYMENT_FACTOR
        print(f"Earning calculated: {earnings}")
        return earnings
    
    async def _update_status(self, task: Task, completion_info: master_pb2.TaskCompleteRequest):
        # Update status of all output files to completed
        self.logger.info(f"Updating status of task {task.task_id} to completed")
        for data in task.data_files:
            data.status = DataStatus.completed

        task.avg_memory_bytes = completion_info.average_memory_bytes
        task.total_active_time = completion_info.total_time_elapsed
        task.completed_at = datetime.now()
        task.status = TaskStatus.completed 
        await self.task_repo.update(task)
    
    async def _notify_dependents(self, task: Task):
        self.logger.info(f"Notifying dependents of task {task.task_id}")
        worker_client = WorkerClient()
        dependents_info = await self.get_task_output_dependencies_with_node_info(task.task_id)
        await self.task_repo.session.refresh(task, ['node'])
        node = task.node
        for dependent in dependents_info:
            if dependent.node_id is None:
                #TODO: what to do if the dependent task is not assigned to a node?
                self.logger.error(f"Dependent task not assigned to a node: {dependent.dependent_task_id}")
                continue

            response = await worker_client.notify_data(worker_pb2.DataNotification(
                task_id=dependent.dependent_task_id,
                data_id=dependent.data_id,
                data_name=dependent.file_name,
                ip_address=node.ip_address,
                port=node.port
            ), dependent.node_ip_address, dependent.node_port)

            if not response:
                self.logger.error(f"Failed to notify dependent task: {dependent.dependent_task_id}")
                continue

    
    async def complete_task(self, completion_info: master_pb2.TaskCompleteRequest) -> bool:
        try:
            task = await self.task_repo.get_task_with_data_files_with_node_info(completion_info.task_id)
            if not task:
                self.logger.error(f"Task not found: {completion_info.task_id}")
                return None
            job_id = task.job_id
            earnings = Earning(
                task_id=task.task_id,
                amount=await self._calculate_earning(completion_info, task),
                status=EarningStatus.pending,
                node_id=task.node_id,
                user_id=task.node.user_id
            )

            await self._update_status(task, completion_info)
            await self._notify_dependents(task)
            await self.earnings_repo.create(earnings)
            self.logger.info(f"Earning created: {earnings}")

            return job_id

        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Error completing task: {e}")
            return None
        
    async def get_total_node_ram(self, node_id: int) -> int:
        tasks = await self.task_repo.get_running_tasks_by_node_id(node_id)
        total_ram = 0
        for task in tasks:
            total_ram += task.required_ram
        return total_ram
    
    # async def get_tasks_sorted_by_created_at()
        
    async def get_tasks_with_job_id(self, job_id: int) -> List[Task]:
        return await self.task_repo.get_tasks_with_job_id(job_id)
    
    async def start_task(self, request: master_pb2.TaskStartRequest) -> bool:
        task = await self.task_repo.get_by_id(request.task_id)
        if not task:
            self.logger.error(f"Task not found: {request.task_id}")
            return False
        task.status = TaskStatus.running
        task.started_at = datetime.fromtimestamp(request.start_time)
        await self.task_repo.update(task)
        return True

def get_task_service(session: AsyncSession) -> TaskService:
    return TaskService(TaskRepository(session), EarningsRepository(session))