from app.db.models.scheme import Earning
from app.db.repositories.base import BaseRepository
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

class EarningsRepository(BaseRepository[Earning]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Earning)

    async def get_by_task_id(self, task_id: int) -> Earning:
        statement = select(Earning).where(Earning.task_id == task_id)
        result = await self.session.execute(statement)
        return result.scalars().first()

def get_earnings_repository(session: AsyncSession) -> EarningsRepository:
    return EarningsRepository(session)