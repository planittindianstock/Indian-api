from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RenderApiSettings(BaseSettings):
    APP_NAME: str = "Indian Trader Signal Ingest API"
    APP_VERSION: str = "1.0.0"
    LOG_LEVEL: str = "INFO"

    SIGNAL_API_KEY: str = ""
    ADMIN_API_KEY: str = ""
    MONGODB_URI: str = ""
    MONGODB_DB_NAME: str = "indian_trader"
    MONGODB_SIGNALS_COLLECTION: str = "signals"
    MONGODB_SIGNAL_EVENTS_COLLECTION: str = "signal_events"

    ALLOWED_ORIGINS: List[str] = Field(default_factory=lambda: ["*"])
    DEFAULT_PAGE_SIZE: int = 50
    MAX_PAGE_SIZE: int = 200
    ENABLE_AI_ADVISOR: bool = False
    ENABLE_RUNTIME_WORKERS: bool = False
    SIGNAL_SCAN_INTERVAL_SECONDS: int = 90
    SIGNAL_SCAN_SYMBOLS: List[str] = Field(default_factory=lambda: ["RELIANCE.NS", "TCS.NS", "INFY.NS"])
    LABEL_LOOKAHEAD_DAYS: int = 3

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        enable_decoding=False,
    )

    @field_validator("ALLOWED_ORIGINS", mode="before")
    @classmethod
    def parse_allowed_origins(cls, value):
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("SIGNAL_SCAN_SYMBOLS", mode="before")
    @classmethod
    def parse_scan_symbols(cls, value):
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value


settings = RenderApiSettings()
