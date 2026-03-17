from sqlalchemy import String, BigInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.skill_types import SkillType

class Type(Base):
    __tablename__ = 'types'
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    
    # Relación con skill_types
    skill_types: Mapped[list["SkillType"]] = relationship("SkillType", back_populates="type")
    
    def __repr__(self) -> str:
        return f"<Type(id={self.id}, name='{self.name}')>"