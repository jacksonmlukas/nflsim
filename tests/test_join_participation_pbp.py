import duckdb
import pytest


def _has_participation(con) -> bool:
    sql = (
        "SELECT COUNT(*) FROM information_schema.views "
        "WHERE table_name='participation'"
    )
    return bool(con.execute(sql).fetchone()[0])


def test_join_rate_by_season():
    con = duckdb.connect("data/nflsim.duckdb")
    if not _has_participation(con):
        pytest.skip("participation view not present")
    sql = (
        "SELECT t.season, "
        "COUNT(*) AS part_rows, "
        "SUM(CASE WHEN p.play_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_rows, "
        "1.0 * SUM(CASE WHEN p.play_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) "
        "AS match_rate "
        "FROM participation t "
        "LEFT JOIN pbp p "
        "ON COALESCE(p.game_id, p.old_game_id) = "
        "   COALESCE(t.nflverse_game_id, t.old_game_id) "
        "AND p.play_id = t.play_id "
        "GROUP BY 1 ORDER BY 1"
    )
    df = con.execute(sql).fetchdf()
    for _, row in df.iterrows():
        season = int(row["season"])
        rate = float(row["match_rate"])
        if season >= 2023:
            assert rate >= 0.999
        else:
            assert rate >= 0.98
