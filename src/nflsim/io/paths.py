from pathlib import Path

from ..settings import settings

DATA_ROOT = Path(settings.data_root)


def dpath(*parts: str) -> Path:
    """Build a path under the data root."""
    return DATA_ROOT.joinpath(*parts)
