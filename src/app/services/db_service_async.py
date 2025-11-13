import asyncio
from datetime import date, datetime
from typing import Any, Dict, Optional, Sequence, Union

from sqlalchemy import Row, create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from src.app.models import City, Location, WeatherAggregate


class AsyncDBService:
    """Async database helper for common read/write operations.

    This service provides asynchronous-friendly methods to interact with the
    application's SQL database using SQLAlchemy's synchronous engine by
    delegating blocking calls to a threadpool via `asyncio.to_thread`.

    Attributes:
        db_url (str): Database connection URL.
        _engine: SQLAlchemy Engine instance.
        _SessionLocal: Session factory.
    """

    def __init__(self, db_url: str) -> None:
        self.db_url: str = db_url
        self._engine = create_engine(db_url, echo=False, future=True)
        self._SessionLocal = sessionmaker(
            bind=self._engine, expire_on_commit=False
        )

    async def get_session(self) -> Session:
        """Return a new SQLAlchemy `Session` instance.

        Note: This method is synchronous in nature but kept async for API
        compatibility with the rest of the codebase.

        Returns:
            Session: New SQLAlchemy session from the configured factory.
        """
        return self._SessionLocal()

    async def get_all_cities_with_coordinates(
        self,
    ) -> Sequence[Row[tuple[str, float, float]]]:
        """Return a sequence of (city_name, latitude, longitude) rows.

        The query is executed in a separate thread to avoid blocking the
        event loop.
        """

        def _query() -> Sequence[Row[tuple[str, float, float]]]:
            with self._SessionLocal() as session:
                result = session.execute(
                    select(
                        City.name, Location.latitude, Location.longitude
                    ).join(City.location)
                )
                return result.all()

        return await asyncio.to_thread(_query)

    async def get_city_by_name(self, city_name: str) -> Optional[City]:
        """Lookup a City record by its name.

        Args:
            city_name (str): Name of the city to search for.

        Returns:
            Optional[City]: City instance if found, otherwise None.
        """

        def _query() -> Optional[City]:
            with self._SessionLocal() as session:
                result = session.execute(
                    select(City).filter(City.name == city_name)
                )
                return result.scalar_one_or_none()

        return await asyncio.to_thread(_query)

    async def get_weather_aggregate(
        self, city_id: int, target_date: Union[date, datetime]
    ) -> Optional[WeatherAggregate]:
        """Fetch a WeatherAggregate record for a city and date.

        Args:
            city_id (int): Database id of the city.
            target_date (date | datetime): Date for which to fetch
            the aggregate.

        Returns:
            Optional[WeatherAggregate]: Found aggregate or None.
        """

        def _query() -> Optional[WeatherAggregate]:
            with self._SessionLocal() as session:
                result = session.execute(
                    select(WeatherAggregate).filter_by(
                        city_id=city_id, date=target_date
                    )
                )
                return result.scalar_one_or_none()

        return await asyncio.to_thread(_query)

    async def add_weather_aggregate(
        self,
        city_id: int,
        target_date: Union[date, datetime],
        agg: Dict[str, Any],
    ) -> WeatherAggregate:
        """Insert a WeatherAggregate record and return the persisted object.

        Args:
            city_id (int): Reference to the city id.
            target_date (date | datetime): Date of the aggregate.
            agg (Dict[str, Any]): Mapping of aggregate fields
            (temp_min, temp_max, etc.).

        Returns:
            WeatherAggregate: The created and refreshed SQLAlchemy
            model instance.
        """

        def _query() -> WeatherAggregate:
            with self._SessionLocal() as session:
                record = WeatherAggregate(
                    city_id=city_id, date=target_date, **agg
                )
                session.add(record)
                session.commit()
                session.refresh(record)
                return record

        return await asyncio.to_thread(_query)
