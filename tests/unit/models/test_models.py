from sqlalchemy import Date, DateTime, Float, String
from sqlalchemy.orm import attributes

from src.app.models import City, Location, WeatherAggregate


def test_city_table_columns_and_relationships() -> None:
    # Arrange
    table = City.__table__

    # Act
    cols = set(table.columns.keys())
    code_col = table.columns["code"]
    name_col = table.columns["name"]
    created_at_col = table.columns["created_at"]

    # Assert
    expected = {"id", "code", "name", "created_at"}
    assert expected.issubset(cols)

    assert isinstance(code_col.type, String)
    assert getattr(code_col.type, "length", None) == 2

    assert isinstance(name_col.type, String)
    assert getattr(name_col.type, "length", None) == 50

    assert isinstance(created_at_col.type, DateTime)

    assert isinstance(
        getattr(City, "location"), attributes.InstrumentedAttribute
    )
    assert isinstance(
        getattr(City, "weather_aggregates"), attributes.InstrumentedAttribute
    )


def test_location_table_columns_and_fks() -> None:
    # Arrange
    table = Location.__table__

    # Act
    cols = set(table.columns.keys())
    lat_col = table.columns["latitude"]
    lon_col = table.columns["longitude"]
    city_id_col = table.columns["city_id"]

    # Assert
    expected = {
        "id",
        "openweather_id",
        "city_id",
        "latitude",
        "longitude",
        "created_at",
    }
    assert expected.issubset(cols)

    assert isinstance(lat_col.type, Float)
    assert isinstance(lon_col.type, Float)

    fks = [str(fk.column) for fk in city_id_col.foreign_keys]
    assert any("cities.id" in fk for fk in fks)

    assert isinstance(
        getattr(Location, "city"), attributes.InstrumentedAttribute
    )


def test_weather_aggregate_table_columns_constraints_and_defaults() -> None:
    # Arrange
    table = WeatherAggregate.__table__

    # Act
    cols = set(table.columns.keys())

    # Assert
    expected = {
        "id",
        "city_id",
        "date",
        "temp_min",
        "temp_max",
        "temp_avg",
        "humidity_avg",
        "precipitation_sum",
        "wind_speed_avg",
        "readings_count",
        "created_at",
    }
    assert expected.issubset(cols)

    assert isinstance(table.columns["date"].type, Date)
    assert isinstance(table.columns["created_at"].type, DateTime)

    constraint_names = {
        getattr(c, "name", None) for c in getattr(table, "constraints", [])
    }
    assert "uq_weather_aggregates_cities_id_date" in constraint_names

    rc = table.columns["readings_count"]
    assert rc.server_default is not None
    assert "0" in str(rc.server_default.arg)

    assert isinstance(
        getattr(WeatherAggregate, "city"), attributes.InstrumentedAttribute
    )
