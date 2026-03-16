from typing import Optional
from datetime import datetime
from sqlalchemy import String, Boolean, Float, DateTime, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.config.db import Base
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.data_models.companies import Company
    from db.data_models.countries import Country
    from db.data_models.locations import Location
    from db.data_models.vias import Via
    from db.data_models.schedule_types import ScheduleType
    from db.data_models.short_titles import ShortTitle
    from db.data_models.job_skills import JobPostSkill

class JobPost(Base):
    __tablename__ = 'job_posts'
    __table_args__ = (
        Index('ix_job_posts_company_id', 'company_id'),
        Index('ix_job_posts_country_id', 'country_id'),
        Index('ix_job_posts_location_id', 'location_id'),
        Index('ix_job_posts_via_id', 'via_id'),
        Index('ix_job_posts_schedule_type_id', 'schedule_type_id'),
        Index('ix_job_posts_short_title_id', 'short_title_id'),
    )
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    # Foreign Keys
    company_id: Mapped[Optional[int]] = mapped_column(ForeignKey('companies.id'), nullable=True)
    country_id: Mapped[Optional[int]] = mapped_column(ForeignKey('countries.id'), nullable=True)
    location_id: Mapped[Optional[int]] = mapped_column(ForeignKey('locations.id'), nullable=True)
    via_id: Mapped[Optional[int]] = mapped_column(ForeignKey('vias.id'), nullable=True)
    schedule_type_id: Mapped[Optional[int]] = mapped_column(ForeignKey('schedule_types.id'), nullable=True)
    short_title_id: Mapped[Optional[int]] = mapped_column(ForeignKey('short_titles.id'), nullable=True)
    
    # Campos de datos
    job_title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    search_location: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    job_posted_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    
    # Campos booleanos
    job_work_from_home: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    job_no_degree_mention: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    job_health_insurance: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    
    # Campos de salario
    salary_rate: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    salary_year_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    salary_hour_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    
    # Relaciones
    company: Mapped[Optional["Company"]] = relationship("Company", back_populates="job_posts")
    country: Mapped[Optional["Country"]] = relationship("Country", back_populates="job_posts")
    location: Mapped[Optional["Location"]] = relationship("Location", back_populates="job_posts")
    via: Mapped[Optional["Via"]] = relationship("Via", back_populates="job_posts")
    schedule_type: Mapped[Optional["ScheduleType"]] = relationship("ScheduleType", back_populates="job_posts")
    short_title: Mapped[Optional["ShortTitle"]] = relationship("ShortTitle", back_populates="job_posts")
    job_post_skills: Mapped[list["JobPostSkill"]] = relationship("JobPostSkill", back_populates="job_post", cascade="all, delete-orphan")
    
    def __repr__(self) -> str:
        return f"<JobPost(id={self.id}, job_title='{self.job_title}')>"
