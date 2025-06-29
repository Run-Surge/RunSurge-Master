from fastapi import APIRouter, Depends, HTTPException

from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.node import get_node_service
from app.services.earnings import get_earnings_service
import traceback

router = APIRouter()

@router.get("/")
async def get_statistics(
    session: AsyncSession = Depends(get_db),
):
    try:
        node_service = get_node_service(session)
        nodes = await node_service.get_all_nodes()
        earnings_service = get_earnings_service(session)
        earnings = await earnings_service.get_all_earnings_amount()
        return {"nodes": len(nodes), "earnings": earnings}
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to fetch statistics")







