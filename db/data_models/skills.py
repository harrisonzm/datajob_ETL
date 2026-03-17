from sqlalchemy import String, BigInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.skill_types import SkillType
    from db.data_models.job_skills import JobPostSkill

class Skill(Base):
    __tablename__ = 'skills'
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    
    # Relaciones
    skill_types: Mapped[list["SkillType"]] = relationship("SkillType", back_populates="skill")
    job_post_skills: Mapped[list["JobPostSkill"]] = relationship("JobPostSkill", back_populates="skill")
    
    def __repr__(self) -> str:
        return f"<Skill(id={self.id}, name='{self.name}')>"
