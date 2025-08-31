import os

from ..checks.validate import validate_and_tag_pbp
from ..data.ingest_fastR import run as ingest_pbp
from ..io.duck import register_views


def main():
    start = int(os.getenv("START_SEASON", "2014"))
    end = int(os.getenv("END_SEASON", "2024"))

    # PBP always
    ingest_pbp(start, end)
    validate_and_tag_pbp()

    # Optional participation (2016+) when enabled
    if os.getenv("ENABLE_PARTICIPATION", "0").lower() in {"1", "true", "yes", "y"}:
        from ..checks.validate import validate_and_tag_participation
        from ..data.ingest_participation import run as ingest_part

        part_start = max(2016, start)
        ingest_part(part_start, end)
        validate_and_tag_participation()

    # Register views (participation only if files exist)
    register_views()


if __name__ == "__main__":
    main()
