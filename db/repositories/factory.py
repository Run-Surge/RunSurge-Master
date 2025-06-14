from sqlmodel import Session
from fastapi import Depends
from db.session import get_session
from .job import JobRepository
from .node import NodeRepository
from .user import UserRepository
from .task import TaskRepository

class RepositoryFactory:
    def __init__(self, session: Session):
        self.session = session
        self._job_repo = None
        self._node_repo = None
        self._user_repo = None
        self._task_repo = None

    @property
    def user(self) -> UserRepository:
        if self._user_repo is None:
            self._user_repo = UserRepository(self.session)
        return self._user_repo
        
    @property
    def job(self) -> JobRepository:
        if self._job_repo is None:
            self._job_repo = JobRepository(self.session)
        return self._job_repo
        
    @property
    def node(self) -> NodeRepository:
        if self._node_repo is None:
            self._node_repo = NodeRepository(self.session)
        return self._node_repo
    
    @property
    def task(self) -> TaskRepository:
        if self._task_repo is None:
            self._task_repo = TaskRepository(self.session)
        return self._task_repo
    

    

def get_repositories(session: Session = Depends(get_session)) -> RepositoryFactory:
    return RepositoryFactory(session)