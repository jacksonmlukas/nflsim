from pydantic import BaseSettings
class Settings(BaseSettings):
    data_root: str = "data"
    duckdb_path: str = "data/nflsim.duckdb"
    class Config:
        env_file = ".env"
settings = Settings()
