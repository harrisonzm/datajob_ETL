from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.job_posts import JobPost

class Via(Base):
    __tablename__ = 'vias'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    
    # Relación con job_posts
    job_posts: Mapped[list["JobPost"]] = relationship("JobPost", back_populates="via")
    
    def __repr__(self) -> str:
        return f"<Via(id={self.id}, name='{self.name}')>"
