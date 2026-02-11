from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from database import Base
from datetime import datetime
from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey

    
class UploadData(Base):
    __tablename__ = "upload_data"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    mapping_config = Column(JSONB)
    operator_pipeline = Column(JSONB)
    create_at = Column(DateTime, default=datetime.utcnow)
    update_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    raw_data = relationship("UploadRawData", backref="upload_data")


class UploadRawData(Base):
    __tablename__ = "upload_raw_data"

    id = Column(Integer, primary_key=True, index=True)
    upload_id = Column(Integer, ForeignKey(UploadData.id))
    data = Column(JSONB, nullable=False)
    create_at = Column(DateTime, default=datetime.utcnow)