from sqlmodel import Session, select
from typing import Optional, List, TypeVar, Generic, Type
import uuid

T=TypeVar('T')

class BaseRepository(Generic[T]):
    def __init__ (self,session: Session, model: Type[T]):
        self.session = session
        self.model = model

    def create(self, obj: T) -> T:
        self.session.add(obj)
        self.session.commit()
        self.session.refresh(obj)
        return obj

    def get_by_id(self, id: uuid.UUID) -> Optional[T]:
        return self.session.get(self.model, id)

    def delete(self, obj: T) -> None:
        self.session.delete(obj)
        self.session.commit()

    def get_all(self) -> List[T]:
        return self.session.exec(select(self.model)).all()
