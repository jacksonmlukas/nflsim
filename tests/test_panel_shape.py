from pathlib import Path

import pandas as pd
import pytest


def test_team_week_panel_exists_and_has_cols():
    p = Path("data/processed/features/team_week_panel/season=2024/panel.parquet")
    if not p.exists():
        pytest.skip("team-week panel missing for 2024")
    df = pd.read_parquet(p)
    cols = {
        "season",
        "week",
        "team",
        "plays",
        "plays_def",
        "pass_rate",
        "pass_rate_faced",
    }
    assert cols.issubset(df.columns)
    assert len(df) >= 500
