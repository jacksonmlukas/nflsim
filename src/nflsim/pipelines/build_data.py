import os

from ..checks.validate import validate_and_tag_pbp
from ..data.ingest_fastR import run as ingest_pbp
from ..io.duck import register_views


def main():
    start = int(os.getenv("START_SEASON", "2014"))
    end = int(os.getenv("END_SEASON", "2024"))
    ingest_pbp(start, end)
    validate_and_tag_pbp()
    register_views()


if __name__ == "__main__":
    main()
