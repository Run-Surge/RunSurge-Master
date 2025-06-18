from app.db.repositories.task import TaskRepository
from app.schemas.task import TaskCreate, TaskUpdate
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.scheme import Task
import logging

### Tasks are created internally, not using endpoints

class TaskService:
    def __init__(self, task_repo: TaskRepository):
        self.task_repo = task_repo

    async def create_task(self, task_data: TaskCreate) -> Task:
        ## we may add validation here, or return true/false
        return await self.task_repo.create_task(task_data)

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


def get_task_service(session: AsyncSession) -> TaskService:
    return TaskService(TaskRepository(session))