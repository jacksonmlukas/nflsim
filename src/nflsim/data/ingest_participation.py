from pathlib import Path
import pandas as pd

_BASE = "https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{season}.parquet"


def run(
    start_season: int = 2016,
    end_season: int = 2024,
    out_root: str = "data/processed/participation",
):
    seasons = list(range(int(start_season), int(end_season) + 1))
    out_base = Path(out_root)
    for y in seasons:
        if y < 2016:
            print(f"skip {y} (<2016 availability)")
            continue
        url = _BASE.format(season=y)
        df = pd.read_parquet(url)
        if "season" not in df.columns:
            df["season"] = int(y)
        out = out_base / f"season={y}" / "participation.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)
        print(f"wrote {out}")
