from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import relationship

from src.app.models.base import Base


class WeatherAggregate(Base):
    __tablename__ = "weather_aggregates"
    __table_args__ = (
        UniqueConstraint(
            "city_id", "date", name="uq_weather_aggregates_cities_id_date"
        ),
        {"schema": "weather"},
    )

    id = Column(Integer, primary_key=True)
    city_id = Column(
        Integer,
        ForeignKey("weather.cities.id", ondelete="CASCADE"),
        nullable=False,
    )
    date = Column(Date, nullable=False)
    temp_min = Column(Float)
    temp_max = Column(Float)
    temp_avg = Column(Float)
    humidity_avg = Column(Float)
    precipitation_sum = Column(Float)
    wind_speed_avg = Column(Float)
    readings_count = Column(Integer, server_default=text("0"))
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    city = relationship("City", back_populates="aggregates")
