from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.skills import Skill

class SkillType(Base):
    __tablename__ = 'skill_types'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    
    # Relación con skills
    skills: Mapped[list["Skill"]] = relationship("Skill", back_populates="skill_type")
    
    def __repr__(self) -> str:
        return f"<SkillType(id={self.id}, name='{self.name}')>"
