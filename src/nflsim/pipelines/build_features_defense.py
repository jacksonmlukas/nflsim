from __future__ import annotations

import os
from pathlib import Path

import duckdb

from ..io.duck import register_views

OUT_ROOT = Path("data/processed/features/defense_team_week")

SQL = r"""
WITH j AS (
  SELECT
    p.season,
    p.week,
    p.game_id,
    p.play_id,
    p.posteam,        -- offense team
    p.defteam,        -- defense team
    COALESCE(p."pass", CASE WHEN p.play_type = 'pass' THEN 1 ELSE 0 END) AS is_pass,
    p.epa,
    p.success,
    t.offense_personnel,
    t.defense_personnel,
    t.number_of_pass_rushers,
    t.was_pressure,
    lower(COALESCE(t.defense_man_zone_type, '')) AS mz_type,
    lower(COALESCE(t.defense_coverage_type, '')) AS cov_type
  FROM participation t
  JOIN pbp p
    ON COALESCE(p.game_id, p.old_game_id) = COALESCE(t.nflverse_game_id, t.old_game_id)
   AND p.play_id = t.play_id
),
parsed AS (
  SELECT
    *,
    CAST(NULLIF(regexp_extract(offense_personnel, '(\d+)\s*RB', 1), '') AS INT) AS rb,
    CAST(NULLIF(regexp_extract(offense_personnel, '(\d+)\s*TE', 1), '') AS INT) AS te,
    CAST(NULLIF(regexp_extract(offense_personnel, '(\d+)\s*WR', 1), '') AS INT) AS wr
  FROM j
),
agg AS (
  SELECT
    season,
    week,
    defteam AS defense_team,
    COUNT(*)                                       AS plays_def,
    AVG(CASE WHEN is_pass = 1 THEN 1 ELSE 0 END)   AS pass_rate_faced,
    AVG(number_of_pass_rushers)                    AS avg_rushers_def,
    AVG(CASE WHEN was_pressure THEN 1 ELSE 0 END)  AS pressure_rate_def,
    -- approximate blitz: 5+ rushers on pass plays
    AVG(CASE WHEN is_pass = 1 AND number_of_pass_rushers >= 5 THEN 1 ELSE 0 END) AS blitz_rate_def,
    AVG(CASE WHEN mz_type = 'man'  OR cov_type = 'man'  THEN 1 ELSE 0 END)        AS man_rate_def,
    AVG(CASE WHEN mz_type = 'zone' OR cov_type = 'zone' THEN 1 ELSE 0 END)        AS zone_rate_def,
    -- personnel usage faced (from offense)
    AVG(CASE WHEN rb = 1 AND te = 1 AND wr = 3 THEN 1 ELSE 0 END) AS rate_11_faced,
    AVG(CASE WHEN rb = 1 AND te = 2 AND wr = 2 THEN 1 ELSE 0 END) AS rate_12_faced,
    AVG(CASE WHEN rb = 1 AND te = 3 THEN 1 ELSE 0 END) AS rate_13_faced,
    AVG(CASE WHEN rb = 1 AND COALESCE(te,0) = 0 AND wr = 4 THEN 1 ELSE 0 END) AS rate_10_faced,
    -- performance allowed
    AVG(epa)                                         AS epa_per_play_allowed,
    AVG(CASE WHEN is_pass = 1 THEN epa END)          AS epa_per_pass_allowed,
    AVG(CASE WHEN is_pass = 0 THEN epa END)          AS epa_per_rush_allowed,
    AVG(CASE WHEN is_pass = 1 THEN success END)      AS pass_success_allowed,
    AVG(CASE WHEN is_pass = 0 THEN success END)      AS rush_success_allowed
  FROM parsed
  GROUP BY 1,2,3
)
SELECT * FROM agg
WHERE season BETWEEN ? AND ?
ORDER BY season, week, defense_team
"""


def main() -> None:
    start = int(os.getenv("START_SEASON", "2016"))
    end = int(os.getenv("END_SEASON", "2024"))
    register_views()
    con = duckdb.connect("data/nflsim.duckdb")
    df = con.execute(SQL, [start, end]).fetchdf()

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    for season, sdf in df.groupby("season"):
        out = OUT_ROOT / f"season={season}" / "features.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        sdf.to_parquet(out, index=False)
        print(f"wrote {out} ({len(sdf)} rows)")


if __name__ == "__main__":
    main()
