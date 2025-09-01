from pathlib import Path

import pandas as pd
import pytest


def test_defense_team_week_columns():
    p = Path("data/processed/features/defense_team_week/season=2024/features.parquet")
    if not p.exists():
        pytest.skip("defense features parquet missing for 2024")
    df = pd.read_parquet(p)
    required = {
        "season",
        "week",
        "defense_team",
        "plays_def",
        "pass_rate_faced",
        "avg_rushers_def",
        "pressure_rate_def",
        "blitz_rate_def",
        "man_rate_def",
        "zone_rate_def",
        "rate_11_faced",
        "rate_12_faced",
        "rate_13_faced",
        "rate_10_faced",
        "epa_per_play_allowed",
        "epa_per_pass_allowed",
        "epa_per_rush_allowed",
        "pass_success_allowed",
        "rush_success_allowed",
    }
    assert required.issubset(df.columns)
    assert df["plays_def"].min() >= 1
    bounded = [
        "pass_rate_faced",
        "man_rate_def",
        "zone_rate_def",
        "pressure_rate_def",
        "blitz_rate_def",
    ]
    for col in bounded:
        assert ((df[col] >= 0) & (df[col] <= 1)).all()
