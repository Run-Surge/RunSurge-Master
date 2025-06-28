from app.db.models.scheme import Payment
from app.db.repositories.base import BaseRepository
from sqlalchemy.ext.asyncio import AsyncSession

class PaymentRepository(BaseRepository[Payment]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Payment)

def get_payment_repository(session: AsyncSession) -> PaymentRepository:
    return PaymentRepository(session)