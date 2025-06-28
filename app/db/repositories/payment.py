from app.db.models.scheme import Payment
from app.db.repositories.base import BaseRepository
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

class PaymentRepository(BaseRepository[Payment]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Payment)
    
    async def get_payment_by_job_id(self, job_id: int):
        statement = select(Payment).where(Payment.job_id == job_id)
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()

def get_payment_repository(session: AsyncSession) -> PaymentRepository:
    return PaymentRepository(session)