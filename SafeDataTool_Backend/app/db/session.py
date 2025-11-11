# database.py
from sqlmodel import SQLModel, Session, create_engine

from app.config import get_settings

settings = get_settings()

echo_sql = settings.environment.lower() == "development"
engine = create_engine(settings.database_url, echo=echo_sql)


def init_db() -> None:
    SQLModel.metadata.create_all(engine)


def get_session() -> Session:
    with Session(engine) as session:
        yield session
