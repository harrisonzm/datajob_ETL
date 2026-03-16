from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.countries import Country
    from db.data_models.job_posts import JobPost

class Location(Base):
    __tablename__ = 'locations'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    location: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    country_id: Mapped[Optional[int]] = mapped_column(ForeignKey('countries.id'), nullable=True)
    
    # Relaciones
    country: Mapped[Optional["Country"]] = relationship("Country", back_populates="locations")
    job_posts: Mapped[list["JobPost"]] = relationship("JobPost", back_populates="location")
    
    def __repr__(self) -> str:
        return f"<Location(id={self.id}, location='{self.location}')>"
