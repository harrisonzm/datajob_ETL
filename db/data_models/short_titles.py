from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.job_posts import JobPost

class ShortTitle(Base):
    __tablename__ = 'short_titles'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    
    # Relación con job_posts
    job_posts: Mapped[list["JobPost"]] = relationship("JobPost", back_populates="short_title")
    
    def __repr__(self) -> str:
        return f"<ShortTitle(id={self.id}, title='{self.title}')>"
