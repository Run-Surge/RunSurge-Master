from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import Optional, List, TypeVar, Generic, Type

T=TypeVar('T')

class BaseRepository(Generic[T]):
    def __init__(self, session: Session, model: Type[T]):
        self.session = session
        self.model = model

    async def create(self, obj: T) -> T:
        self.session.add(obj)
        await self.session.commit()
        await self.session.refresh(obj)
        return obj

    async def get_by_id(self, id: int) -> Optional[T]:
        return await self.session.get(self.model, id)

    async def delete(self, obj: T) -> None:
        self.session.delete(obj)
        await self.session.commit()

    async def get_all(self) -> List[T]:
        result = await self.session.execute(select(self.model))
        return result.scalars().all()
