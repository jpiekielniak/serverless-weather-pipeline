from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.app.models.base import Base

if TYPE_CHECKING:
    from .city import City


class WeatherAggregate(Base):
    __tablename__ = "weather_aggregates"
    __table_args__ = (
        UniqueConstraint(
            "city_id", "date", name="uq_weather_aggregates_cities_id_date"
        ),
        {"schema": "weather"},
    )

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    city_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("weather.cities.id", ondelete="CASCADE"),
        nullable=False,
    )
    date: Mapped[date] = mapped_column(Date, nullable=False)
    temp_min: Mapped[float] = mapped_column(Float)
    temp_max: Mapped[float] = mapped_column(Float)
    temp_avg: Mapped[float] = mapped_column(Float)
    humidity_avg: Mapped[float] = mapped_column(Float)
    precipitation_sum: Mapped[float] = mapped_column(Float)
    wind_speed_avg: Mapped[float] = mapped_column(Float)
    readings_count: Mapped[int] = mapped_column(
        Integer, server_default=text("0")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    city: Mapped["City"] = relationship(
        "City", back_populates="weather_aggregates"
    )
