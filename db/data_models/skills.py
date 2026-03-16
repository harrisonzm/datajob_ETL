from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.skill_types import SkillType
    from db.data_models.job_skills import JobPostSkill

class Skill(Base):
    __tablename__ = 'skills'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    skill_type_id: Mapped[Optional[int]] = mapped_column(ForeignKey('skill_types.id'), nullable=True)
    
    # Relaciones
    skill_type: Mapped[Optional["SkillType"]] = relationship("SkillType", back_populates="skills")
    job_post_skills: Mapped[list["JobPostSkill"]] = relationship("JobPostSkill", back_populates="skill")
    
    def __repr__(self) -> str:
        return f"<Skill(id={self.id}, name='{self.name}')>"
