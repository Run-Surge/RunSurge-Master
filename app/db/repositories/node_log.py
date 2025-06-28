from app.db.models.scheme import NodeLog
from app.db.repositories.base import BaseRepository
from sqlalchemy.ext.asyncio import AsyncSession

class   NodeLogRepository(BaseRepository[NodeLog]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, NodeLog)


def get_node_log_repository(session: AsyncSession) -> NodeLogRepository:
    return NodeLogRepository(session)