from pathlib import Path

import duckdb

from ..settings import settings


def connect():
    Path(settings.duckdb_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(settings.duckdb_path)


def register_views(con=None):
    con = con or connect()
    con.execute(
        "CREATE OR REPLACE VIEW pbp AS "
        "SELECT * FROM read_parquet('data/processed/pbp/season=*/pbp.parquet')"
    )
    return con
