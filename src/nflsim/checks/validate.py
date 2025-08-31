from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import TypedDict

import pandas as pd

from .schemas import (
    validate_participation,
    validate_pbp,
)  # <- make sure both are imported


class SeasonMeta(TypedDict):
    rows: int
    columns: list[str]
    hash: str
    path: str


class Report(TypedDict):
    seasons: dict[str, SeasonMeta]
    created_at: int


def _hash(path: Path) -> str:
    h = hashlib.blake2b(digest_size=16)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _validate_partitioned(
    root: str, filename: str, validator: Callable[[pd.DataFrame], pd.DataFrame]
) -> Report:
    base = Path(root)
    seasons = sorted(
        p.name.split("=", 1)[1] for p in base.glob("season=*") if p.is_dir()
    )
    report: Report = {"seasons": {}, "created_at": int(time.time())}
    for s in seasons:
        p = base / f"season={s}" / filename
        if not p.exists():
            continue
        df = pd.read_parquet(p)
        res = validator(df)
        meta: SeasonMeta = {
            "rows": len(res),
            "columns": list(res.columns),
            "hash": _hash(p),
            "path": str(p),
        }
        (p.parent / "_SUCCESS").write_text("")
        (p.parent / "metadata.json").write_text(json.dumps(meta, indent=2))
        report["seasons"][s] = meta
    (base / "REPORT.json").write_text(json.dumps(report, indent=2))
    return report


def validate_and_tag_pbp(root: str = "data/processed/pbp") -> Report:
    return _validate_partitioned(root, "pbp.parquet", validate_pbp)


def validate_and_tag_participation(
    root: str = "data/processed/participation",
) -> Report:
    return _validate_partitioned(root, "participation.parquet", validate_participation)
