from app.db.repositories.earnings import EarningsRepository
from app.db.repositories.earnings import get_earnings_repository
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.scheme import Earning, EarningStatus
from typing import List
from app.db.models.scheme import Node

class EarningsService:
    def __init__(self, earnings_repo: EarningsRepository):
        self.earnings_repo = earnings_repo

    async def create_earning(self, earning: Earning):
        return await self.earnings_repo.create(earning)
    async def update_earning_status(self, earning_id: int):
        earning = await self.earnings_repo.get_by_id(earning_id)
        earning.status = EarningStatus.paid
        await self.earnings_repo.update(earning)

    async def get_paid_earnings(self, nodes: List[Node]):
        return sum(sum(earning.amount for earning in node.earnings if earning.status == EarningStatus.paid) for node in nodes)
    async def get_pending_earnings(self, nodes: List[Node]):
        return sum(sum(earning.amount for earning in node.earnings if earning.status == EarningStatus.pending) for node in nodes)
    async def get_earning_status_by_task_id(self, task_id: int):
        earning = await self.earnings_repo.get_by_task_id(task_id)
        return earning.status if earning else None
    async def get_earning_amount_by_task_id(self, task_id: int):
        earning = await self.earnings_repo.get_by_task_id(task_id)
        return earning.amount if earning else None
    
    async def get_all_earnings_amount(self):
        earnings = await self.earnings_repo.get_all()
        return sum(earning.amount for earning in earnings)

def get_earnings_service(session: AsyncSession) -> EarningsService:
    return EarningsService(get_earnings_repository(session))