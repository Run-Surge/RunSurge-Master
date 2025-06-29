from app.db.repositories.payment import PaymentRepository
from app.db.repositories.payment import get_payment_repository
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.scheme import Payment

class PaymentService:
    def __init__(self, payment_repo: PaymentRepository):
        self.payment_repo = payment_repo

    async def create_payment(self, payment: Payment):
        return await self.payment_repo.create(payment)
    async def get_payment_by_job_id(self, job_id: int):
        try:
            return await self.payment_repo.get_payment_by_job_id(job_id)
        except Exception as e:
            print(e)
            return None
    
def get_payment_service(session: AsyncSession) -> PaymentService:
    return PaymentService(get_payment_repository(session))