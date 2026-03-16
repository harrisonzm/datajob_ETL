from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.job_posts import JobPost

class ScheduleType(Base):
    __tablename__ = 'schedule_types'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    
    # Relación con job_posts
    job_posts: Mapped[list["JobPost"]] = relationship("JobPost", back_populates="schedule_type")
    
    def __repr__(self) -> str:
        return f"<ScheduleType(id={self.id}, name='{self.name}')>"
