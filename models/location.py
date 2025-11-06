from sqlalchemy import Column, Integer, String, Float, JSON, TIMESTAMP, text
from sqlalchemy.orm import relationship

from models.base import Base


class Location(Base):
    __tablename__ = "locations"
    __table_args__ = {"schema": "weather"}

    id = Column(Integer, primary_key=True)
    openweather_id = Column(Integer, unique=True)
    name = Column(String, nullable=False)
    country = Column(String(2))
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    timezone = Column(String)
    extra_metadata = Column("metadata", JSON)
    created_at = Column(TIMESTAMP, server_default=text("now()"))

    aggregates = relationship("WeatherAggregate", back_populates="location")
