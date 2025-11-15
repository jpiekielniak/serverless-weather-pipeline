from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from sqlalchemy import DateTime, Float, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.app.models.base import Base

if TYPE_CHECKING:
    from .city import City


class Location(Base):
    """Geographical location information associated with a City.

    Represents a single location row containing latitude/longitude and an
    optional external openweather id. The model is mapped to the
    `weather.locations` table.

    Attributes:
        id (int): Primary key.
        openweather_id (Optional[int]): External provider id.
        city_id (int): Foreign key to `cities.id`.
        latitude (float): Latitude in decimal degrees.
        longitude (float): Longitude in decimal degrees.
        created_at (datetime): Record creation timestamp (UTC).
    """

    __tablename__ = "locations"
    __table_args__ = {"schema": "weather"}

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    openweather_id: Mapped[Optional[int]] = mapped_column(Integer, unique=True)
    city_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("weather.cities.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    city: Mapped["City"] = relationship("City", back_populates="location")
