from app.db.repositories.data import DataRepository, InputDataRepository
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.data import DataCreate
from protos import master_pb2
from app.utils.utils import get_data_path
from typing import Optional 
from app.db.models.scheme import InputData

class DataService:
    def __init__(self, data_repo: DataRepository):
        self.data_repo = data_repo
    # -1 means data is not provided by any node (i.e. data is in the master node)
    async def create_data(self, file_name: str, parent_task_id: Optional[int] = None):
        print("in service",file_name, parent_task_id)
        try:
            if parent_task_id is None:
                data = await self.data_repo.create_data(file_name)
            else:
                data = await self.data_repo.create_data(file_name, parent_task_id)
            return data
        except Exception as e:
            print("error in service",e)
            raise e

    async def get_data_path(self, data_id: int):
        data = await self.data_repo.get_by_id(data_id)
        job = await self.data_repo.get_data_job_by_parent_task(data_id)
        if job is not None:
            return get_data_path(data.file_name, job)
        
        job = await self.data_repo.get_data_job_by_dependent_tasks(data_id)
        if job is not None:
            return get_data_path(data.file_name, job)
        
        raise Exception("Data has no parent task or dependent tasks, Can't determine job id")

    async def update_data(self, data_id: int, data_request: master_pb2.MasterDataNotification):
        data = await self.data_repo.get_by_id(data_id)

    async def create_input_data(self, job_id: int, chunk_index: int, total_chunks: int):
        return await self.data_repo.create_input_data(job_id, chunk_index, total_chunks)

    async def get_input_data(self, job_id: int):
        return await self.data_repo.get_input_data(job_id)
    
    async def update_input_data(self, input_data: InputData):
        return await self.data_repo.update_input_data(input_data)
    
    async def get_file(self, data_id: int):
        data = await self.data_repo.get_by_id(data_id)
        return data.file_name

def get_data_service(session: AsyncSession) -> DataService:
    return DataService(DataRepository(session))


class InputDataService:
    def __init__(self, input_data_repo: InputDataRepository):
        self.input_data_repo = input_data_repo

    async def create_input_data(self, job_id: int, chunk_index: int, total_chunks: int,file_name: str):
        return await self.input_data_repo.create_input_data(job_id, chunk_index, total_chunks,file_name)
    
    async def update_input_data(self, input_data: InputData):
        return await self.input_data_repo.update_input_data(input_data)
    
    async def get_input_data(self, job_id: int):
        return await self.input_data_repo.get_input_data(job_id)
    
def get_input_data_service(session: AsyncSession) -> InputDataService:
    return InputDataService(InputDataRepository(session))