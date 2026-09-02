from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Binance ingestion ---
    binance_ws_url: str
    binance_rest_url: str
    symbols: list[str]
    redis_url: str

    database_url: str


settings = Settings()