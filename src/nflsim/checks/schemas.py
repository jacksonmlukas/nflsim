import pandas as pd
import pandera.pandas as pa


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
                int, checks=[pa.Check.ge(1), pa.Check.le(22)], coerce=True
            ),
            "posteam": pa.Column(object, nullable=True),
            "defteam": pa.Column(object, nullable=True),
            "play_type": pa.Column(object, nullable=True),
            "yardline_100": pa.Column(
                float, nullable=True, checks=[pa.Check.ge(0), pa.Check.le(100)]
            ),
            "down": pa.Column(float, nullable=True),
            "ydstogo": pa.Column(float, nullable=True, checks=[pa.Check.ge(0)]),
            "ep": pa.Column(float, nullable=True),
            "wp": pa.Column(
                float, nullable=True, checks=[pa.Check.ge(0), pa.Check.le(1)]
            ),
            "spread_line": pa.Column(float, nullable=True),
            "total_line": pa.Column(float, nullable=True),
            "roof": pa.Column(object, nullable=True),
            "wind": pa.Column(float, nullable=True),
            "temperature": pa.Column(float, nullable=True),
        },
        coerce=True,
    )
    missing = set(schema.columns.keys()) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    return schema.validate(df, lazy=True)


def validate_participation(df: pd.DataFrame) -> pd.DataFrame:
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
                int, checks=[pa.Check.ge(1), pa.Check.le(22)], coerce=True
            ),
            "personnel_offense": pa.Column(object, nullable=True),
            "personnel_defense": pa.Column(object, nullable=True),
            "defenders_in_box": pa.Column(
                float, nullable=True, checks=[pa.Check.ge(0), pa.Check.le(11)]
            ),
            "number_of_pass_rushers": pa.Column(
                float, nullable=True, checks=[pa.Check.ge(0), pa.Check.le(11)]
            ),
        },
        coerce=True,
    )
    missing = set(schema.columns.keys()) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    return schema.validate(df, lazy=True)
