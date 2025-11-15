from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base declarative class for SQLAlchemy models in the application.

    Use this class as the declarative base for all ORM models so that they
    share the same metadata and configuration (schema, naming, etc.).
    """

    pass
