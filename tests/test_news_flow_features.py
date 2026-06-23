from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows or [])

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _NoClose:
    def __init__(self, con):
        self._con = con

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self._con.executemany(*args, **kwargs)

    def close(self):
        return None


class _CountingCon:
    def __init__(self, con):
        self._con = con
        self.recent_embedding_queries = 0
        self.story_embedding_write_batches = 0
        self.event_feature_write_batches = 0
        self.event_feature_select_then_write_queries = 0

    def execute(self, sql, params=None):
        sql_text = " ".join(str(sql or "").split()).lower()
        if (
            "select event_id, symbol, dim, vector, availability_ts_ms" in sql_text
            and "from news_story_embeddings" in sql_text
        ):
            self.recent_embedding_queries += 1
        if "select 1 from news_event_features where event_id" in sql_text:
            self.event_feature_select_then_write_queries += 1
        return self._con.execute(sql, tuple(params or ()))

    def executemany(self, sql, seq_of_params):
        sql_text = " ".join(str(sql or "").split()).lower()
        rows = list(seq_of_params or [])
        if "insert into news_story_embeddings" in sql_text:
            self.story_embedding_write_batches += 1
        if "insert into news_event_features" in sql_text:
            self.event_feature_write_batches += 1
        return self._con.executemany(sql, rows)

    def close(self):
        return None


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _make_news_db(news_flow):
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            ts_ms INTEGER,
            timestamp INTEGER,
            event_type TEXT,
            symbol TEXT,
            source TEXT,
            title TEXT,
            body TEXT,
            source_id TEXT,
            event_key TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE news_event_features (
            event_id INTEGER PRIMARY KEY,
            ts_ms INTEGER,
            symbol TEXT,
            sentiment_score REAL DEFAULT 0.0,
            novelty_score REAL DEFAULT 0.0,
            is_duplicate INTEGER DEFAULT 0,
            finbert_score REAL,
            finbert_neg REAL,
            meta_json TEXT
        )
        """
    )
    news_flow.ensure_news_flow_tables(con)
    return con


def _insert_event(con, event_id: int, ts_ms: int, title: str, body: str = "", *, symbol: str = "AAPL", sentiment: float = 0.0, neg: float = 0.0):
    con.execute(
        """
        INSERT INTO events(id, ts_ms, timestamp, event_type, symbol, source, title, body, source_id, event_key)
        VALUES (?, ?, ?, 'news', ?, 'unit', ?, ?, ?, ?)
        """,
        (int(event_id), int(ts_ms), int(ts_ms), str(symbol), str(title), str(body), f"src-{event_id}", f"event-{event_id}"),
    )
    con.execute(
        """
        INSERT INTO news_event_features(event_id, ts_ms, symbol, sentiment_score, finbert_score, finbert_neg)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (int(event_id), int(ts_ms), str(symbol), float(sentiment), float(sentiment), float(neg)),
    )


def test_news_novelty_identical_and_unrelated_texts() -> None:
    (news_flow,) = _reload("engine.data.news_flow")
    cfg = news_flow.NewsEmbeddingConfig(backend="hashing", model_name="hashing-v1")
    vectors = news_flow.encode_news_texts(
        [
            "Apple earnings beat expectations",
            "Apple earnings beat expectations",
            "quantum shipping copper eclipse",
        ],
        cfg,
    )

    identical_novelty, identical_sim, identical_stale = news_flow.novelty_from_vector(vectors[1], [vectors[0]])
    unrelated_novelty, unrelated_sim, unrelated_stale = news_flow.novelty_from_vector(vectors[2], [vectors[0]])

    assert identical_sim > 0.99
    assert identical_novelty < 0.01
    assert identical_stale is True
    assert unrelated_sim < 0.40
    assert unrelated_novelty > 0.60
    assert unrelated_stale is False


def test_news_flow_mixed_embedding_space_guard_and_batch_cap(monkeypatch) -> None:
    (news_flow,) = _reload("engine.data.news_flow")
    cfg = news_flow.NewsEmbeddingConfig(backend="hashing", model_name="hashing-v1")
    con = _make_news_db(news_flow)
    same_vec = news_flow.encode_news_texts(["Apple repeats the same headline"], cfg)[0]
    con.execute(
        """
        INSERT INTO news_story_embeddings(
          event_id, symbol, publish_ts_ms, availability_ts_ms, source,
          embedding_backend, model_name, dim, vector, text_hash,
          novelty_score, max_similarity, stale_flag
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            99,
            "AAPL",
            1_000,
            1_000,
            "unit",
            "other_backend",
            "other-model",
            int(same_vec.size),
            news_flow.vector_to_bytes(same_vec),
            "prior",
            0.0,
            1.0,
            1,
        ),
    )
    _insert_event(con, 1, 2_000, "Apple repeats the same headline")
    _insert_event(con, 2, 3_000, "Apple repeats the same headline")
    monkeypatch.setattr(news_flow, "connect", lambda readonly=False: _NoClose(con))
    monkeypatch.setattr(news_flow, "run_write_txn", lambda fn, **_kwargs: fn(con))

    first = news_flow.process_news_flow_batch(limit=1, config=cfg, now_ms=2_500)
    second = news_flow.process_news_flow_batch(limit=1, config=cfg, now_ms=3_500)

    row1 = con.execute(
        "SELECT novelty_score, stale_flag, max_similarity FROM news_story_embeddings WHERE event_id = 1 AND embedding_backend = ?",
        (cfg.backend,),
    ).fetchone()
    row2 = con.execute(
        "SELECT novelty_score, stale_flag, max_similarity FROM news_story_embeddings WHERE event_id = 2 AND embedding_backend = ?",
        (cfg.backend,),
    ).fetchone()
    assert first["written"] == 1
    assert second["written"] == 1
    assert row1[0] > 0.99
    assert row1[1] == 0
    assert row1[2] == 0.0
    assert row2[0] < 0.01
    assert row2[1] == 1
    assert row2[2] > 0.99

    class _FakeCon:
        def __init__(self):
            self.params = None

        def execute(self, _sql, params=None):
            self.params = tuple(params or ())
            return _Cursor([])

    fake = _FakeCon()
    news_flow._recent_embedding_rows(fake, symbol="AAPL", availability_ts_ms=10_000, config=cfg)
    assert fake.params[-1] == 200


def test_news_flow_no_lookahead_uses_embedding_availability(monkeypatch) -> None:
    (news_flow,) = _reload("engine.data.news_flow")
    cfg = news_flow.NewsEmbeddingConfig(backend="hashing", model_name="hashing-v1")
    con = _make_news_db(news_flow)
    _insert_event(con, 1, 2_000, "Apple faces a fresh probe", sentiment=-0.8, neg=0.9)
    _insert_event(con, 2, 5_000, "Apple faces a fresh probe", sentiment=-0.8, neg=0.9)
    monkeypatch.setattr(news_flow, "connect", lambda readonly=False: _NoClose(con))
    monkeypatch.setattr(news_flow, "run_write_txn", lambda fn, **_kwargs: fn(con))

    news_flow.process_news_flow_batch(limit=2, config=cfg, now_ms=6_000)

    before, before_meta, before_available = news_flow.resolve_news_flow_features(con, symbol="AAPL", ts_ms=3_000, config=cfg)
    after, after_meta, after_available = news_flow.resolve_news_flow_features(con, symbol="AAPL", ts_ms=6_000, config=cfg)

    assert before_available is True
    assert before_meta["latest_availability_ts_ms"] == 2_000
    assert before_meta["event_count_24h"] == 1
    assert before["fresh_neg_news_flag"] == 1.0
    assert after_available is True
    assert after_meta["latest_availability_ts_ms"] == 5_000
    assert after_meta["event_count_24h"] == 2
    assert after["news_stale_share_24h"] == 0.5


def test_news_flow_batches_recent_embedding_query_and_writes(monkeypatch) -> None:
    (news_flow,) = _reload("engine.data.news_flow")
    cfg = news_flow.NewsEmbeddingConfig(backend="hashing", model_name="hashing-v1")
    con = _make_news_db(news_flow)
    counting = _CountingCon(con)
    _insert_event(con, 1, 2_000, "Apple repeats one acquisition headline")
    _insert_event(con, 2, 3_000, "Apple repeats one acquisition headline")
    _insert_event(con, 3, 4_000, "Apple announces unrelated developer tools")
    monkeypatch.setattr(news_flow, "connect", lambda readonly=False: _NoClose(counting))
    monkeypatch.setattr(news_flow, "run_write_txn", lambda fn, **_kwargs: fn(counting))

    result = news_flow.process_news_flow_batch(limit=10, config=cfg, now_ms=5_000)

    rows = con.execute(
        """
        SELECT event_id, stale_flag, matched_event_id
        FROM news_story_embeddings
        WHERE embedding_backend=?
        ORDER BY event_id
        """,
        (cfg.backend,),
    ).fetchall()
    assert result["written"] == 3
    assert result["batch_size"] == 3
    assert result["recent_embedding_queries"] == 1
    assert result["story_embedding_write_batches"] == 1
    assert result["event_feature_write_batches"] == 1
    assert result["write_batches"] == 2
    assert result["embedding_db_round_trips"] == 3
    assert counting.recent_embedding_queries == 1
    assert counting.story_embedding_write_batches == 1
    assert counting.event_feature_write_batches == 1
    assert counting.event_feature_select_then_write_queries == 0
    assert rows[0] == (1, 0, None)
    assert rows[1] == (2, 1, 1)
    assert rows[2][0] == 3


def test_news_flow_one_shot_transaction_failure_is_retryable(monkeypatch) -> None:
    (news_flow,) = _reload("engine.data.news_flow")
    cfg = news_flow.NewsEmbeddingConfig(backend="hashing", model_name="hashing-v1")
    con = _make_news_db(news_flow)
    _insert_event(con, 1, 2_000, "Apple publishes a new product update")
    monkeypatch.setattr(news_flow, "connect", lambda readonly=False: _NoClose(con))

    def _failing_write_txn(_fn, **_kwargs):
        raise RuntimeError("simulated_db_slowdown")

    monkeypatch.setattr(news_flow, "run_write_txn", _failing_write_txn)
    with pytest.raises(RuntimeError, match="simulated_db_slowdown"):
        news_flow.process_news_flow_batch(limit=1, config=cfg, now_ms=2_500)

    pending = news_flow._fetch_pending_events(con, config=cfg, limit=10)
    assert [row["event_id"] for row in pending] == [1]
    assert con.execute("SELECT COUNT(*) FROM news_story_embeddings").fetchone()[0] == 0

    monkeypatch.setattr(news_flow, "run_write_txn", lambda fn, **_kwargs: fn(con))
    retry = news_flow.process_news_flow_batch(limit=1, config=cfg, now_ms=3_000)

    assert retry["written"] == 1
    assert con.execute("SELECT COUNT(*) FROM news_story_embeddings WHERE event_id=1").fetchone()[0] == 1
    assert news_flow._fetch_pending_events(con, config=cfg, limit=10) == []


def test_news_flow_registry_round_trip_and_job_registered(monkeypatch) -> None:
    monkeypatch.setenv("USE_NEWS_FLOW_FEATURES", "1")
    (feature_registry,) = _reload("engine.strategy.feature_registry")
    (job_registry,) = _reload("engine.runtime.job_registry")

    ids = list(feature_registry.NEWS_FLOW_FEATURE_IDS)
    assert ids == ["news_novelty_max_24h", "news_stale_share_24h", "news_velocity_z", "fresh_neg_news_flag"]
    assert feature_registry.FEATURE_GROUPS["news_flow"] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert feature_registry.expected_columns(ids, fallback_to_default=False) == ids
    assert "news_flow" in feature_registry.feature_set_tag_from_ids(ids).split("+")
    assert job_registry.ALLOWED_JOBS["process_news_flow"][3]["cadence_seconds"] == 900
