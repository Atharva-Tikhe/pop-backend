# models.py
from sqlalchemy import Column, String, DateTime, JSON, Boolean, Integer, Float, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
import uuid
from datetime import datetime

Base = declarative_base()

# class Pipeline(Base):
#     __tablename__ = "pipelines"

#     internal_id = Column(Integer, primary_key=True, autoincrement=True)

#     id = Column(String, nullable=False)
    
#     status = Column(String, nullable=False, default="pending")
    
#     manifest = Column(JSON, nullable=False)

#     created_at = Column(DateTime, default=datetime.utcnow)
#     updated_at = Column(DateTime, default=datetime.utcnow)

#     # optional but useful
#     name = Column(String, nullable=True)
#     input = Column(String, nullable=True)
#     success = Column(Boolean, nullable=True)


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(String, primary_key=True, index = True)          # runId

    status = Column(String)

    name = Column(String)
    input = Column(String)


    # start_time = Column(DateTime)
    # end_time = Column(DateTime)

    duration = Column(Float)

    success = Column(Boolean)

    manifest = Column(JSON)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
            DateTime(timezone=True),
            server_default=func.now(),
            onupdate=func.now()
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