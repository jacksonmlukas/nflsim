from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

OUT_ROOT = Path("data/processed/features/team_week_panel")


def build_one(season: int) -> int:
    off_p = Path(
        f"data/processed/features/offense_team_week/season={season}/features.parquet"
    )
    def_p = Path(
        f"data/processed/features/defense_team_week/season={season}/features.parquet"
    )
    if not (off_p.exists() and def_p.exists()):
        return 0
    off = pd.read_parquet(off_p)
    deff = pd.read_parquet(def_p).rename(columns={"defense_team": "team"})
    panel = off.merge(
        deff,
        on=["season", "week", "team"],
        how="outer",
        suffixes=("_off", "_def"),
        validate="one_to_one",
    )
    out = OUT_ROOT / f"season={season}" / "panel.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out, index=False)
    print(f"wrote {out} ({len(panel)} rows)")
    return len(panel)


def main() -> None:
    start = int(os.getenv("START_SEASON", "2016"))
    end = int(os.getenv("END_SEASON", "2024"))
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    total = 0
    for season in range(start, end + 1):
        total += build_one(season)
    print(f"total rows: {total}")


if __name__ == "__main__":
    main()
