# models.py
from sqlalchemy import Column, String, DateTime, JSON, Boolean, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base
import uuid
from datetime import datetime

Base = declarative_base()

class Pipeline(Base):
    __tablename__ = "pipelines"

    internal_id = Column(Integer, primary_key=True, autoincrement=True)

    id = Column(String, nullable=False)
    
    status = Column(String, nullable=False, default="pending")
    
    manifest = Column(JSON, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    # optional but useful
    name = Column(String, nullable=True)
    input = Column(String, nullable=True)
    success = Column(Boolean, nullable=True)

