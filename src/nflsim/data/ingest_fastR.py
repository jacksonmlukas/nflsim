from pathlib import Path
import nfl_data_py as nfl


def run(
    start_season: int = 2014,
    end_season: int = 2024,
    out_root: str = "data/processed/pbp",
):
    seasons = list(range(int(start_season), int(end_season) + 1))
    out_base = Path(out_root)
    for y in seasons:
        df = nfl.import_pbp_data([y], downcast=True, cache=False)
        out = out_base / f"season={y}" / "pbp.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)
        print(f"wrote {out}")
