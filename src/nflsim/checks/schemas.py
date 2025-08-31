import pandas as pd
import pandera.pandas as pa


# ---------- PBP schema ----------
def validate_pbp(df: pd.DataFrame) -> pd.DataFrame:
    if "temp" in df.columns and "temperature" not in df.columns:
        df = df.rename(columns={"temp": "temperature"})

    schema = pa.DataFrameSchema(
        {
            "game_id": pa.Column(
                str, checks=pa.Check.str_length(min_value=1), coerce=True
            ),
            "play_id": pa.Column(int, checks=pa.Check.ge(1), coerce=True),
            "season": pa.Column(
                int, checks=[pa.Check.ge(1999), pa.Check.le(2100)], coerce=True
            ),
            "week": pa.Column(
                int,
                checks=[pa.Check.ge(1), pa.Check.le(22)],
                coerce=True,
                required=False,
                nullable=True,
            ),
            "posteam": pa.Column(object, nullable=True, required=False),
            "defteam": pa.Column(object, nullable=True, required=False),
            "play_type": pa.Column(object, nullable=True, required=False),
            "yardline_100": pa.Column(
                float,
                nullable=True,
                checks=[pa.Check.ge(0), pa.Check.le(100)],
                required=False,
            ),
            "down": pa.Column(float, nullable=True, required=False),
            "ydstogo": pa.Column(
                float, nullable=True, checks=[pa.Check.ge(0)], required=False
            ),
            "ep": pa.Column(float, nullable=True, required=False),
            "wp": pa.Column(
                float,
                nullable=True,
                checks=[pa.Check.ge(0), pa.Check.le(1)],
                required=False,
            ),
            "spread_line": pa.Column(float, nullable=True, required=False),
            "total_line": pa.Column(float, nullable=True, required=False),
            "roof": pa.Column(object, nullable=True, required=False),
            "wind": pa.Column(float, nullable=True, required=False),
            "temperature": pa.Column(float, nullable=True, required=False),
        },
        coerce=True,
        strict=False,
    )

    required_cols = {
        n for n, col in schema.columns.items() if getattr(col, "required", True)
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return schema.validate(df, lazy=True)


# ---------- Participation schema ----------
def validate_participation(df: pd.DataFrame) -> pd.DataFrame:
    if "nflverse_game_id" not in df.columns and "game_id" in df.columns:
        df = df.rename(columns={"game_id": "nflverse_game_id"})

    if "personnel_offense" not in df.columns and "offense_personnel" in df.columns:
        df = df.rename(columns={"offense_personnel": "personnel_offense"})
    if "personnel_defense" not in df.columns and "defense_personnel" in df.columns:
        df = df.rename(columns={"defense_personnel": "personnel_defense"})

    # sanitize obviously invalid numeric sentinels (e.g., 44) to NA before checks
    for col in ["number_of_pass_rushers", "defenders_in_box"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[(df[col] < 0) | (df[col] > 11), col] = pd.NA

    schema = pa.DataFrameSchema(
        {
            "nflverse_game_id": pa.Column(
                str, checks=pa.Check.str_length(min_value=1), coerce=True
            ),
            "play_id": pa.Column(int, checks=pa.Check.ge(1), coerce=True),
            "season": pa.Column(
                int, checks=[pa.Check.ge(2016), pa.Check.le(2100)], coerce=True
            ),
            "week": pa.Column(
                int,
                checks=[pa.Check.ge(1), pa.Check.le(22)],
                coerce=True,
                required=False,
                nullable=True,
            ),
            "personnel_offense": pa.Column(object, nullable=True),
            "personnel_defense": pa.Column(object, nullable=True),
            "defenders_in_box": pa.Column(
                float,
                nullable=True,
                checks=[pa.Check.ge(0), pa.Check.le(11)],
                required=False,
            ),
            "number_of_pass_rushers": pa.Column(
                float,
                nullable=True,
                checks=[pa.Check.ge(0), pa.Check.le(11)],
                required=False,
            ),
        },
        coerce=True,
        strict=False,
    )

    required_cols = {
        n for n, col in schema.columns.items() if getattr(col, "required", True)
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return schema.validate(df, lazy=True)
