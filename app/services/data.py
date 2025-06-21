from app.db.repositories.data import DataRepository
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.data import DataCreate

class DataService:
    def __init__(self, data_repo: DataRepository):
        self.data_repo = data_repo
    # -1 means data is not provided by any node (i.e. data is in the master node)
    async def create_data(self, file_name: str, job_id: int, provider_id: int =- 1):
        try:
            ## reutrn as object
            data = await self.data_repo.create_data(file_name, job_id, provider_id)
            print("in service",data.data_id)
            return data
        except Exception as e:
            print("error in service",e)
            raise e
    
    async def create_data_request(self, data_request: DataCreate):
        return await self.data_repo.create_data(data_request.file_name, data_request.job_id, -1)

def get_data_service(session: AsyncSession) -> DataService:
    return DataService(DataRepository(session))