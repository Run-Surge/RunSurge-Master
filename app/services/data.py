from app.db.repositories.data import DataRepository
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.data import DataCreate
from protos import master_pb2
from app.utils.utils import get_data_path
from typing import Optional

class DataService:
    def __init__(self, data_repo: DataRepository):
        self.data_repo = data_repo
    # -1 means data is not provided by any node (i.e. data is in the master node)
    async def create_data(self, file_name: str, job_id: int, parent_task_id: Optional[int] = None):
        print("in service",file_name, job_id, parent_task_id)
        try:
            if parent_task_id is None:
                data = await self.data_repo.create_data(file_name, job_id)
            else:
                data = await self.data_repo.create_data(file_name, job_id, parent_task_id)
            return data
        except Exception as e:
            print("error in service",e)
            raise e

    async def get_data_path(self, data_id: int):
        data = await self.data_repo.get_by_id(data_id)
        return get_data_path(data)

    async def create_data_request(self, data_request: DataCreate):
        return await self.data_repo.create_data(data_request.file_name, data_request.job_id, -1)
    

    async def update_data(self, data_id: int, data_request: master_pb2.MasterDataNotification):
        data = await self.data_repo.get_by_id(data_id)


    

def get_data_service(session: AsyncSession) -> DataService:
    return DataService(DataRepository(session))