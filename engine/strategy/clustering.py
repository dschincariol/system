"""
FILE: clustering.py

Groups related events into lightweight narrative clusters using embedding
similarity. The implementation is intentionally SQLite-native and incremental so
it can run inside the existing runtime without a separate vector store.
"""

import os
import time
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from engine.runtime.storage import connect, run_write_txn

THRESH = float(os.environ.get("CLUSTER_SIM_THRESHOLD", "0.82"))
MAX_RECENT = int(os.environ.get("CLUSTER_MAX_RECENT", "500"))


SCHEMA = """
CREATE TABLE IF NOT EXISTS narrative_clusters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_ts_ms INTEGER NOT NULL,
  updated_ts_ms INTEGER NOT NULL,
  n INTEGER NOT NULL,
  dim INTEGER NOT NULL,
  centroid BLOB NOT NULL,
  title_hint TEXT
);

CREATE TABLE IF NOT EXISTS narrative_members (
  event_id INTEGER PRIMARY KEY,
  cluster_id INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_narr_clusters_updated ON narrative_clusters(updated_ts_ms);
CREATE INDEX IF NOT EXISTS idx_narr_members_cluster ON narrative_members(cluster_id);
"""


def init_clusters_db():
    con = connect()
    try:
        con.executescript(SCHEMA)
        con.commit()
    finally:
        con.close()


def _load_recent_clusters(con):
    rows = con.execute(
        """
        SELECT id, n, dim, centroid, title_hint
        FROM narrative_clusters
        ORDER BY updated_ts_ms DESC
        LIMIT ?
        """,
        (int(MAX_RECENT),),
    ).fetchall()

    out = []
    for cid, n, dim, blob, title_hint in rows or []:
        v = np.frombuffer(blob, dtype=np.float32)
        if v.shape[0] != int(dim):
            continue
        out.append((int(cid), int(n), v, str(title_hint or "")))
    return out


def assign_cluster(event_id: int, ts_ms: int, title: str, vec: np.ndarray):
    init_clusters_db()

    now_ms = int(time.time() * 1000)
    v = np.asarray(vec, dtype=np.float32).reshape(1, -1)

    con = connect()
    try:
        # Already assigned?
        row = con.execute(
            "SELECT cluster_id FROM narrative_members WHERE event_id=?",
            (int(event_id),),
        ).fetchone()
        if row:
            return {"cluster_id": int(row[0]), "action": "exists"}

        clusters = _load_recent_clusters(con)
        if clusters:
            centroids = np.stack([c[2] for c in clusters]).astype(np.float32, copy=False)
            sims = cosine_similarity(v, centroids)[0]
        else:
            sims = []

        best_idx = None
        best_sim = 0.0
        for i, sim in enumerate(sims):
            if float(sim) > float(best_sim):
                best_sim = float(sim)
                best_idx = i

        # Assign to existing cluster
        if best_idx is not None and float(best_sim) >= float(THRESH):
            cid, n, centroid, _hint = clusters[best_idx]

            def _assign_existing(db):
                existing = db.execute(
                    "SELECT cluster_id FROM narrative_members WHERE event_id=?",
                    (int(event_id),),
                ).fetchone()
                if existing:
                    return {"cluster_id": int(existing[0]), "action": "exists"}

                cluster_row = db.execute(
                    "SELECT n, dim, centroid FROM narrative_clusters WHERE id=?",
                    (int(cid),),
                ).fetchone()
                if not cluster_row:
                    return {"cluster_id": int(cid), "action": "missing_cluster"}

                latest_n = int(cluster_row[0] or n)
                latest_dim = int(cluster_row[1] or 0)
                latest_centroid = np.frombuffer(cluster_row[2], dtype=np.float32)
                if latest_dim > 0 and latest_centroid.shape[0] != latest_dim:
                    latest_centroid = centroid

                # Re-read the current centroid under the write transaction so
                # concurrent assignments do not clobber incremental updates.
                n2 = int(latest_n) + 1
                new_centroid = (latest_centroid * float(latest_n) + v.reshape(-1)) / float(n2)
                new_centroid = new_centroid.astype(np.float32, copy=False)

                db.execute(
                    """
                    UPDATE narrative_clusters
                    SET updated_ts_ms=?, n=?, centroid=?, title_hint=?
                    WHERE id=?
                    """,
                    (int(now_ms), int(n2), new_centroid.tobytes(), str(title or "")[:200], int(cid)),
                )
                db.execute(
                    """
                    INSERT OR REPLACE INTO narrative_members(event_id, cluster_id, ts_ms)
                    VALUES (?,?,?)
                    """,
                    (int(event_id), int(cid), int(ts_ms)),
                )
                return {
                    "cluster_id": int(cid),
                    "action": "assigned",
                    "sim": float(best_sim),
                    "threshold": float(THRESH),
                }

            result = run_write_txn(
                _assign_existing,
                table="narrative_clusters",
                operation="assign_cluster_existing",
                context={"event_id": int(event_id), "cluster_id": int(cid)},
            )
            if str((result or {}).get("action") or "") == "exists":
                return {"cluster_id": int(result["cluster_id"]), "action": "exists"}
            if str((result or {}).get("action") or "") == "missing_cluster":
                return {"cluster_id": int(cid), "action": "missing_cluster"}
            return result

        # Create new cluster
        dim = int(v.shape[1])

        def _create_cluster(db):
            existing = db.execute(
                "SELECT cluster_id FROM narrative_members WHERE event_id=?",
                (int(event_id),),
            ).fetchone()
            if existing:
                return {"cluster_id": int(existing[0]), "action": "exists"}

            db.execute(
                """
                INSERT INTO narrative_clusters(created_ts_ms, updated_ts_ms, n, dim, centroid, title_hint)
                VALUES (?,?,?,?,?,?)
                """,
                (int(now_ms), int(now_ms), 1, int(dim), v.reshape(-1).tobytes(), str(title or "")[:200]),
            )
            new_cid = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
            db.execute(
                """
                INSERT OR REPLACE INTO narrative_members(event_id, cluster_id, ts_ms)
                VALUES (?,?,?)
                """,
                (int(event_id), int(new_cid), int(ts_ms)),
            )
            return {
                "cluster_id": int(new_cid),
                "action": "new",
                "sim": float(best_sim),
                "threshold": float(THRESH),
            }

        return run_write_txn(
            _create_cluster,
            table="narrative_clusters",
            operation="assign_cluster_new",
            context={"event_id": int(event_id)},
        )

    finally:
        con.close()
