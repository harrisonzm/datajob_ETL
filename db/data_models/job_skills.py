from sqlalchemy import ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.job_posts import JobPost
    from db.data_models.skills import Skill

class JobPostSkill(Base):
    __tablename__ = 'job_post_skills'
    __table_args__ = (
        UniqueConstraint('job_post_id', 'skill_id', name='uq_job_post_skill'),
        Index('ix_job_post_skills_job_post_id', 'job_post_id'),
        Index('ix_job_post_skills_skill_id', 'skill_id'),
    )
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_post_id: Mapped[int] = mapped_column(ForeignKey('job_posts.id', ondelete='CASCADE'), nullable=False)
    skill_id: Mapped[int] = mapped_column(ForeignKey('skills.id', ondelete='CASCADE'), nullable=False)
    
    # Relaciones
    job_post: Mapped["JobPost"] = relationship("JobPost", back_populates="job_post_skills")
    skill: Mapped["Skill"] = relationship("Skill", back_populates="job_post_skills")
    
    def __repr__(self) -> str:
        return f"<JobPostSkill(job_post_id={self.job_post_id}, skill_id={self.skill_id})>"
