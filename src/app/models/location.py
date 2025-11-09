from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer
from sqlalchemy.orm import relationship

from src.app.models.base import Base


class Location(Base):
    __tablename__ = "locations"
    __table_args__ = {"schema": "weather"}

    id = Column(Integer, primary_key=True)
    openweather_id = Column(Integer, unique=True)
    city_id = Column(
        Integer, ForeignKey("weather.cities.id"), unique=True, nullable=False
    )
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    city = relationship("City", back_populates="location")
