from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.job_posts import JobPost
    from db.data_models.locations import Location

class Country(Base):
    __tablename__ = 'countries'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    
    # Relaciones
    job_posts: Mapped[list["JobPost"]] = relationship("JobPost", back_populates="country")
    locations: Mapped[list["Location"]] = relationship("Location", back_populates="country")
    
    def __repr__(self) -> str:
        return f"<Country(id={self.id}, name='{self.name}')>"
