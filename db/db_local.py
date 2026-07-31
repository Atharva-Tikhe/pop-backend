from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.model import Base, Pipeline
from datetime import datetime

DB_URL = "sqlite://local_db.db"

engine = create_engine(DB_URL, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)



