from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.scheme import Group, Job
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.db.repositories.base import BaseRepository

class GroupRepository(BaseRepository[Group]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Group)

    async def create_group(self, group_name: str, python_file_name: str, aggregator_file_name: str, aggregator_file_path: str, num_of_jobs: int, user_id: int):
        group = Group(group_name=group_name, python_file_name=python_file_name, aggregator_file_name=aggregator_file_name, aggregator_file_path=aggregator_file_path, num_of_jobs=num_of_jobs, user_id=user_id)
        return await self.create(group)
    
    async def get_groups_by_user_id(self, user_id: int):
        statement = select(Group).options(selectinload(Group.jobs)).where(Group.user_id == user_id)
        result = await self.session.execute(statement)
        return result.scalars().all()
    
    async def get_group_by_id(self, group_id: int, user_id: int):
        statement = select(Group).options(selectinload(Group.jobs)).where(Group.group_id == group_id, Group.user_id == user_id)
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()