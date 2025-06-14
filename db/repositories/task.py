from sqlmodel import Session, select
from db.models.scheme import Task, TaskAssignment, TaskStatus
from db.repositories.base import BaseRepository
import uuid

class TaskRepository(BaseRepository[Task]):
    def __init__(self, session: Session):
        super().__init__(session, Task)
    # not tested due to data_id
    def create_task(self, job_id: uuid.UUID, data_id: uuid.UUID, required_ram: int) -> Task:
        task = Task(
            job_id=job_id,
            data_id=data_id,
            required_ram=required_ram,
            status=TaskStatus.pending
        )
        return self.create(task)