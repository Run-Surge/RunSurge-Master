from app.db.models.scheme import Earning
from app.db.repositories.base import BaseRepository
from sqlalchemy.ext.asyncio import AsyncSession

class EarningsRepository(BaseRepository[Earning]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Earning)

def get_earnings_repository(session: AsyncSession) -> EarningsRepository:
    return EarningsRepository(session)