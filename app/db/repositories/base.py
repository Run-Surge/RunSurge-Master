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
        self.session.commit()
        self.session.refresh(obj)
        return obj

    async def get_by_id(self, id: str) -> Optional[T]:
        return self.session.get(self.model, id)

    async def delete(self, obj: T) -> None:
        self.session.delete(obj)
        self.session.commit()

    async def get_all(self) -> List[T]:
        return self.session.execute(select(self.model)).scalars().all()
