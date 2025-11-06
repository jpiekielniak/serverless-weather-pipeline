from sqlalchemy import Column, Integer, ForeignKey, Date, Float, JSON, TIMESTAMP, text, UniqueConstraint
from sqlalchemy.orm import relationship

from models.base import Base


class WeatherAggregate(Base):
    __tablename__ = "weather_aggregates"
    __table_args__ = (
        UniqueConstraint('location_id', 'date', name='uq_weather_aggregates_location_id_date'),
        {"schema": "weather"},
    )

    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey("weather.locations.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    temp_min = Column(Float)
    temp_max = Column(Float)
    temp_avg = Column(Float)
    humidity_avg = Column(Float)
    precipitation_sum = Column(Float)
    wind_speed_avg = Column(Float)
    readings_count = Column(Integer, server_default=text('0'))
    extra_metadata = Column("metadata", JSON)
    created_at = Column(TIMESTAMP, server_default=text("now()"))

    location = relationship("Location", back_populates="aggregates")