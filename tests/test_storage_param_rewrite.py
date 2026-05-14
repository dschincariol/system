import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime.storage_dialect import to_pg_params


def test_rewrites_qmark_parameters():
    assert to_pg_params("SELECT * FROM prices WHERE symbol=? AND ts_ms>?") == (
        "SELECT * FROM prices WHERE symbol=%s AND ts_ms>%s"
    )


def test_preserves_question_marks_inside_string_literals():
    sql = "SELECT '?' AS literal, note FROM t WHERE symbol=? AND text='why?'"
    assert to_pg_params(sql) == "SELECT '?' AS literal, note FROM t WHERE symbol=%s AND text='why?'"


def test_preserves_question_marks_inside_double_quoted_identifiers():
    sql = 'SELECT "weird?column" FROM events WHERE symbol=?'
    assert to_pg_params(sql) == 'SELECT "weird?column" FROM events WHERE symbol=%s'


def test_preserves_question_marks_inside_dollar_quoted_strings():
    sql = "SELECT $tag$does this ? stay$tag$ AS note WHERE symbol=?"
    assert to_pg_params(sql) == "SELECT $tag$does this ? stay$tag$ AS note WHERE symbol=%s"


def test_preserves_question_marks_inside_comments():
    sql = "SELECT 1 -- keep ? in comment\nWHERE symbol=? /* and keep ? here */"
    assert to_pg_params(sql) == "SELECT 1 -- keep ? in comment\nWHERE symbol=%s /* and keep ? here */"


def test_rewrites_positional_placeholder_with_literal_question_neighbors():
    sql = "SELECT '?' AS literal WHERE symbol=?"
    assert to_pg_params(sql) == "SELECT '?' AS literal WHERE symbol=%s"


def test_preserves_json_path_string_literals():
    sql = "SELECT payload_json->>'headline?' FROM events WHERE meta_json->>'source'=?"
    assert to_pg_params(sql) == "SELECT payload_json->>'headline?' FROM events WHERE meta_json->>'source'=%s"


def test_preserves_json_question_operator():
    sql = "SELECT * FROM events WHERE meta_json ? 'source' AND symbol=?"
    assert to_pg_params(sql) == "SELECT * FROM events WHERE meta_json ? 'source' AND symbol=%s"


def test_preserves_json_question_array_operators():
    sql = "SELECT * FROM events WHERE meta_json ?| array['a', 'b'] AND tags ?& array['x'] AND symbol=?"
    assert to_pg_params(sql) == (
        "SELECT * FROM events WHERE meta_json ?| array['a', 'b'] AND tags ?& array['x'] AND symbol=%s"
    )


def test_param_rewrite_cache_is_value_keyed():
    to_pg_params.cache_clear()
    assert to_pg_params("SELECT ?") == "SELECT %s"
    assert to_pg_params("SELECT '?'") == "SELECT '?'"
    assert to_pg_params.cache_info().maxsize == 1024
