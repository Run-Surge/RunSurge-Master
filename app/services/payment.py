from app.db.repositories.payment import PaymentRepository
from app.db.repositories.payment import get_payment_repository
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.scheme import Payment

class PaymentService:
    def __init__(self, payment_repo: PaymentRepository):
        self.payment_repo = payment_repo

    async def create_payment(self, payment: Payment):
        return await self.payment_repo.create(payment)
    
def get_payment_service(session: AsyncSession) -> PaymentService:
    return PaymentService(get_payment_repository(session))