from functools import lru_cache
from typing import List, Union

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore extra env vars instead of raising errors
        populate_by_name=True,  # Allow both field names and aliases
    )

    # Application
    environment: str = "development"
    app_name: str = "SafeData Tool"
    debug: bool = True

    # Database
    database_url: str = "sqlite:///./safedata.db"

    # JWT - Accept both old (SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES) and new naming
    jwt_secret_key: str = Field(
        default="your-secret-key-change-in-production",
        validation_alias=AliasChoices("SECRET_KEY", "JWT_SECRET_KEY", "jwt_secret_key"),
    )
    jwt_algorithm: str = Field(
        default="HS256",
        validation_alias=AliasChoices("ALGORITHM", "JWT_ALGORITHM", "jwt_algorithm"),
    )
    jwt_expire_minutes: int = Field(
        default=1440,  # 24 hours
        validation_alias=AliasChoices("ACCESS_TOKEN_EXPIRE_MINUTES", "JWT_EXPIRE_MINUTES", "jwt_expire_minutes"),
    )

    # CORS - Parse comma-separated string or use default list
    allow_origins: Union[List[str], str] = Field(
        default=["http://localhost:8000", "http://127.0.0.1:8000", "http://localhost:5500", "http://127.0.0.1:5500"],
        validation_alias=AliasChoices("ALLOW_ORIGINS", "allow_origins"),
    )

    @field_validator("allow_origins", mode="before")
    @classmethod
    def parse_allow_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return v

    # SMTP (for OTP emails)
    smtp_email: str = ""
    smtp_password: str = ""

    # File Storage
    data_dir: str = "./data"
    reports_dir: str = "./reports"
    samples_dir: str = "./samples"

    # Pipeline
    max_file_size_mb: int = 100
    pipeline_timeout_seconds: int = 3600  # 1 hour

    # Background Jobs (Celery)
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/0"
    use_background_jobs: bool = False  # Set to True to enable Celery


@lru_cache()
def get_settings() -> Settings:
    return Settings()

