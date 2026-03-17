from sqlalchemy import ForeignKey, BigInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.skills import Skill
    from db.data_models.types import Type

class SkillType(Base):
    __tablename__ = 'skill_types'
    
    skill_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('skills.id', deferrable=True, initially='IMMEDIATE'), primary_key=True, nullable=False)
    type_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('types.id', deferrable=True, initially='IMMEDIATE'), primary_key=True, nullable=False)
    
    # Relaciones
    skill: Mapped["Skill"] = relationship("Skill", back_populates="skill_types")
    type: Mapped["Type"] = relationship("Type", back_populates="skill_types")
    
    def __repr__(self) -> str:
        return f"<SkillType(skill_id={self.skill_id}, type_id={self.type_id})>"