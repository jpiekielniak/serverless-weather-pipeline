from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import relationship

from src.app.models.base import Base


class City(Base):
    __tablename__ = "cities"
    __table_args__ = {"schema": "weather"}

    id = Column(Integer, primary_key=True)
    code = Column(String(2), nullable=False)
    name = Column(String(50), nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    location = relationship("Location", back_populates="city", uselist=False)
    weather_aggregates = relationship(
        "WeatherAggregate", back_populates="city"
    )
