import pandas as pd

from nflsim.checks.schemas import validate_participation


def test_golden_participation_schema():
    df = pd.read_parquet("tests/data/golden_participation_2024.parquet")
    out = validate_participation(df)
    assert len(out) == len(df)
