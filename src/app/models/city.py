from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, List

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.app.models.base import Base

if TYPE_CHECKING:
    from .location import Location
    from .weather_aggregate import WeatherAggregate


class City(Base):
    """Represents a city and its relationships (location and aggregates).

    The City model holds basic identifying information and links to a single
    `Location` and multiple `WeatherAggregate` records. It maps to the
    `weather.cities` table.

    Attributes:
        id (int): Primary key.
        code (str): Country code or short identifier (2 chars).
        name (str): City name.
        created_at (datetime): Record creation timestamp (UTC).
        location (Location): One-to-one relationship to Location.
        weather_aggregates (List[WeatherAggregate]): One-to-many
        relationship to daily aggregates.
    """

    __tablename__ = "cities"
    __table_args__ = {"schema": "weather"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(2), nullable=False)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    location: Mapped["Location"] = relationship(
        "Location",
        back_populates="city",
        uselist=False,
        cascade="all, delete-orphan",
    )

    weather_aggregates: Mapped[List["WeatherAggregate"]] = relationship(
        "WeatherAggregate",
        back_populates="city",
        cascade="all, delete-orphan",
    )
