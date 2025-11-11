from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings


ROOT_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    app_name: str = Field(default="SafeDataTool API", alias="APP_NAME")
    environment: str = Field(default="development", alias="ENVIRONMENT")
    database_url: str = Field(default="sqlite:///./safedata.db", alias="DATABASE_URL")

    allow_origins: list[str] = Field(default_factory=lambda: ["*"], alias="ALLOW_ORIGINS")

    data_dir: Path = Field(default=ROOT_DIR / "data", alias="DATA_DIR")
    raw_data_dir: Path = Field(default=ROOT_DIR / "data" / "raw", alias="RAW_DATA_DIR")
    processed_data_dir: Path = Field(default=ROOT_DIR / "data" / "processed", alias="PROCESSED_DATA_DIR")
    reports_dir: Path = Field(default=ROOT_DIR / "reports", alias="REPORTS_DIR")
    samples_dir: Path = Field(default=ROOT_DIR / "samples", alias="SAMPLES_DIR")

    smtp_sender: str | None = Field(default=None, alias="SMTP_EMAIL")
    smtp_password: str | None = Field(default=None, alias="SMTP_PASSWORD")

    jwt_secret_key: str | None = Field(default=None, alias="SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", alias="ALGORITHM")
    jwt_expire_minutes: int = Field(default=60, alias="ACCESS_TOKEN_EXPIRE_MINUTES")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        populate_by_name = True

    @model_validator(mode="after")
    def ensure_directories(self) -> "Settings":
        paths = [
            self.data_dir,
            self.raw_data_dir,
            self.processed_data_dir,
            self.reports_dir,
            self.samples_dir,
        ]
        for directory in paths:
            directory.mkdir(parents=True, exist_ok=True)
        return self

    def path_for(self, *parts: str, base: Path | None = None) -> Path:
        target_base = base or self.data_dir
        return target_base.joinpath(*parts)


@lru_cache
def get_settings() -> Settings:
    return Settings()

