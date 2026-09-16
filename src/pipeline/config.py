"""Runtime configuration from environment variables."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    brasilapi_base_url: str = "https://brasilapi.com.br"
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 3

    data_dir: Path = Path("./data")
    ingest_date: str | None = Field(
        default=None,
        description="Optional YYYY-MM-DD partition. Empty uses today's UTC date.",
    )

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "br_open_data"
    postgres_user: str = "pipeline"
    postgres_password: str = "pipeline"
    postgres_sslmode: str = "disable"

    def partition_date(self) -> date:
        if self.ingest_date:
            return date.fromisoformat(self.ingest_date)
        return datetime.now(timezone.utc).date()

    def dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password} sslmode={self.postgres_sslmode}"
        )


def load_settings() -> Settings:
    return Settings()
