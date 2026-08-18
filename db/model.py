# models.py
from sqlalchemy import Column, String, DateTime, JSON, Boolean, Integer, Float, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
import uuid
from datetime import datetime

Base = declarative_base()

  
class Cohort(Base):
    __tablename__ = 'cohorts'
    
    id = Column(
        UUID(as_uuid=True), 
        primary_key=True, 
        default = uuid.uuid4
    )
    
    name = Column(String, nullable = False)
    samplesheet = Column(String, nullable = False)
    threshold = Column(String, nullable = False)
    
    status = Column(String, 
                    nullable = False, 
                    default = "SUBMITTED"
                    )
    
    panel = Column(String, 
                   nullable = False, 
                   default = "ALLTOGETHER1")
    
    output_dir = Column(String, nullable = False)
    
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )  
    
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False        
    )
    
    pipelines = relationship(
        "Pipeline",
        back_populates="cohort",
        cascade="all, delete-orphan"
    )
    

class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(String, primary_key=True, index = True)          # runId

    cohort_id = Column(UUID(as_uuid=True), 
                       ForeignKey("cohorts.id"), 
                       nullable=False,
                       index = True)

    status = Column(String)

    name = Column(String)
    input = Column(String)

    sample_name = Column(String, index = True)

    # start_time = Column(DateTime)
    # end_time = Column(DateTime)

    duration = Column(Float)

    success = Column(Boolean)

    manifest = Column(JSON)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    updated_at = Column(
            DateTime(timezone=True),
            server_default=func.now(),
            onupdate=func.now(),
            nullable=False
        )

    cohort = relationship(
        "Cohort", 
        back_populates="pipelines"
    )
    tasks = relationship(
        "PipelineTask",
        back_populates="pipeline",
        cascade="all, delete-orphan"
    )
    
    


class PipelineTask(Base):
    __tablename__ = "pipeline_tasks"
    __table_args__ = (
    UniqueConstraint(
        "pipeline_id",
        "task_id"
        ),
    )

    internal_id = Column(Integer, primary_key=True)

    pipeline_id = Column(
        ForeignKey("pipelines.id"),
        nullable=False,
        index = True
    )

    task_id = Column(Integer)

    process_name = Column(String)

    hash = Column(String)

    status = Column(String)

    workdir = Column(String)

    cpus = Column(Integer)

    runtime = Column(Float)

    pipeline = relationship(
        "Pipeline",
        back_populates="tasks"
    )
    