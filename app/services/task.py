from app.db.repositories.task import TaskRepository
from app.schemas.task import TaskCreate, TaskUpdate, TaskStatus, TaskDataWithNodeInfo, TaskOutputDependentInfo
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.scheme import Task, Data, DataStatus, Node, JobStatus
from typing import List
import logging
from protos import master_pb2, worker_pb2
from app.services.worker_client import WorkerClient
import traceback

### Tasks are created internally, not using endpoints

class TaskService:
    def __init__(self, task_repo: TaskRepository):
        self.task_repo = task_repo

    async def create_task(self, job_id: int, data_ids: list[int], required_ram: int, node_id: int) -> Task:
        ## we may add validation here, or return true/false
        return await self.task_repo.create_task(job_id, data_ids, required_ram, node_id)

    ## This function is not used for endpoints, it is used for internal use only
    ## it should return False if the task is not found or if the task is already assigned to a node
    ## it should return True if the task is assigned to the given node
    async def assign_task_to_node(self, task_id: int, node_id: int) -> Task:
        task = await self.task_repo.get_by_id(task_id)
        if not task:
            logging.error(f"Task not found: {task_id}")
            return False
        if task.node_id is not None:
            logging.error(f"Task already assigned to node: {task.node_id}")
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
    
    async def _update_status(self, task: Task):
        # Update status of all output files to completed
        logging.info(f"Updating status of task {task.task_id} to completed")
        for data in task.data_files:
            data.status = DataStatus.completed

        task.status = TaskStatus.completed 
        await self.task_repo.update(task)
    
    async def _notify_dependents(self, task: Task):
        logging.info(f"Notifying dependents of task {task.task_id}")
        worker_client = WorkerClient()
        dependents_info = await self.get_task_output_dependencies_with_node_info(task.task_id)
        await self.task_repo.session.refresh(task, ['node'])
        node = task.node
        for dependent in dependents_info:
            if dependent.node_id is None:
                #TODO: what to do if the dependent task is not assigned to a node?
                logging.error(f"Dependent task not assigned to a node: {dependent.dependent_task_id}")
                continue

            response = await worker_client.notify_data(worker_pb2.DataNotification(
                task_id=dependent.dependent_task_id,
                data_id=dependent.data_id,
                data_name=dependent.file_name,
                ip_address=node.ip_address,
                port=node.port
            ), dependent.node_ip_address, dependent.node_port)

            if not response:
                logging.error(f"Failed to notify dependent task: {dependent.dependent_task_id}")
                continue

    
    async def complete_task(self, completion_info: master_pb2.TaskCompleteRequest) -> bool:
        try:
            task = await self.task_repo.get_task_with_data_files(completion_info.task_id)
            if not task:
                logging.error(f"Task not found: {completion_info.task_id}")
                return None, None
            
            await self._update_status(task)
            await self._notify_dependents(task)

            return task.job_id

        except Exception as e:
            print(traceback.format_exc())
            logging.error(f"Error completing task: {e}")
            return None, None
        
    async def get_total_node_ram(self, node_id: int) -> int:
        tasks = await self.task_repo.get_running_tasks_by_node_id(node_id)
        total_ram = 0
        for task in tasks:
            total_ram += task.required_ram
        return total_ram
        

def get_task_service(session: AsyncSession) -> TaskService:
    return TaskService(TaskRepository(session))