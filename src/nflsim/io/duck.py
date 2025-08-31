import duckdb
from ..settings import settings

def connect():
    return duckdb.connect(settings.duckdb_path)
