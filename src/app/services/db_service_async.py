import asyncio
from datetime import date, datetime
from typing import Any, Dict, Optional, Sequence, Union

from sqlalchemy import Row, create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from src.app.models import City, Location, WeatherAggregate


class AsyncDBService:
    def __init__(self, db_url: str) -> None:
        self.db_url: str = db_url
        self._engine = create_engine(db_url, echo=False, future=True)
        self._SessionLocal = sessionmaker(
            bind=self._engine, expire_on_commit=False
        )

    async def get_session(self) -> Session:
        return self._SessionLocal()

    async def get_all_cities_with_coordinates(
        self,
    ) -> Sequence[Row[tuple[str, float, float]]]:
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
