from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List, TypeVar, Generic, Type

T=TypeVar('T')

#TODO: check db commits, enable rollback

class BaseRepository(Generic[T]):
    def __init__(self, session: AsyncSession, model: Type[T]):
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
    
    async def update(self, obj: T) -> None:
        await self.session.commit()
        await self.session.refresh(obj)