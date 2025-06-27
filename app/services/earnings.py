from app.db.repositories.earnings import EarningsRepository
from app.db.repositories.earnings import get_earnings_repository
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.scheme import Earning

class EarningsService:
    def __init__(self, earnings_repo: EarningsRepository):
        self.earnings_repo = earnings_repo

    async def create_earning(self, earning: Earning):
        return await self.earnings_repo.create(earning)

def get_earnings_service(session: AsyncSession) -> EarningsService:
    return EarningsService(get_earnings_repository(session))