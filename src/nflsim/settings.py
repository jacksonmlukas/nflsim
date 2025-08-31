from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    data_root: str = "data"
    duckdb_path: str = "data/nflsim.duckdb"
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
