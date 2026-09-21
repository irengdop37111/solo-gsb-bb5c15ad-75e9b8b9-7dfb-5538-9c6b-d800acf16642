"""SQLite access layer.

One connection per request (created by the FastAPI dependency in main.py).
WAL mode + busy_timeout allow concurrent readers and serialized writers;
all mutations happen inside BEGIN IMMEDIATE transactions (see write_txn),
which keeps per-device window state consistent under concurrent requests.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id        TEXT PRIMARY KEY,
    next_expected    INTEGER NOT NULL,          -- next contiguous sequence expected
    gap_since        REAL,                      -- when the current pending gap was first seen
    duplicates_count INTEGER NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    device_id   TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    payload     TEXT NOT NULL,                  -- JSON payload as received
    status      TEXT NOT NULL CHECK (status IN ('committed', 'buffered', 'gap_filled')),
    received_at REAL NOT NULL,
    PRIMARY KEY (device_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_records_device_status ON records(device_id, status);

CREATE TABLE IF NOT EXISTS gaps (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL,
    start_seq    INTEGER NOT NULL,
    end_seq      INTEGER NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('open', 'replaying', 'recovered')),
    replay_count INTEGER NOT NULL DEFAULT 0,
    detected_at  REAL NOT NULL,                 -- when the gap was first observed
    updated_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gaps_device ON gaps(device_id, status);

CREATE TABLE IF NOT EXISTS alert_rules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id   TEXT NOT NULL,
    field       TEXT NOT NULL,              -- top-level payload key to inspect
    op          TEXT NOT NULL CHECK (op IN ('gt', 'lt')),
    threshold   REAL NOT NULL,
    consecutive INTEGER NOT NULL CHECK (consecutive >= 1),
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alert_rules_device ON alert_rules(device_id);

CREATE TABLE IF NOT EXISTS alert_rule_state (
    rule_id    INTEGER PRIMARY KEY REFERENCES alert_rules (id) ON DELETE CASCADE,
    last_seq   INTEGER,                     -- last committed seq evaluated (NULL = none yet)
    streak     INTEGER NOT NULL DEFAULT 0,  -- current run of consecutive violations
    alerted    INTEGER NOT NULL DEFAULT 0,  -- 1 while the current run has fired its alert
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id       INTEGER NOT NULL REFERENCES alert_rules (id) ON DELETE CASCADE,
    device_id     TEXT NOT NULL,
    field         TEXT NOT NULL,            -- rule snapshot, so alerts survive rule changes
    op            TEXT NOT NULL,
    threshold     REAL NOT NULL,
    consecutive   INTEGER NOT NULL,
    start_seq     INTEGER NOT NULL,         -- first seq of the violating run
    end_seq       INTEGER NOT NULL,         -- seq at which the run reached `consecutive`
    trigger_value REAL NOT NULL,            -- field value of the triggering record
    created_at    REAL NOT NULL,
    -- Acknowledgement workflow: every alert is born 'open' and can be
    -- acknowledged exactly once; the first confirmation's identity/timestamp
    -- are frozen for the lifetime of the row.
    status          TEXT NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'acknowledged')),
    acknowledged_by TEXT,                   -- identifier of the first confirmer
    acknowledged_at REAL                    -- server time of the first confirmation
);
CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts(device_id, id);
"""

# Idempotent column migrations for databases created before the alerts table
# gained the acknowledgement columns (e.g. an existing Docker /data volume).
# Safe to re-run: columns already present are skipped.
ALERT_COLUMN_MIGRATIONS = (
    (
        "status",
        "ALTER TABLE alerts ADD COLUMN status TEXT NOT NULL DEFAULT 'open' "
        "CHECK (status IN ('open', 'acknowledged'))",
    ),
    ("acknowledged_by", "ALTER TABLE alerts ADD COLUMN acknowledged_by TEXT"),
    ("acknowledged_at", "ALTER TABLE alerts ADD COLUMN acknowledged_at REAL"),
)


def connect(db_path: str) -> sqlite3.Connection:
    # check_same_thread=False: FastAPI may run a request's dependency setup,
    # endpoint and teardown on different threadpool threads. Each connection
    # is still confined to a single request, and BEGIN IMMEDIATE serializes
    # writers, so this stays safe.
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate_alert_columns(conn)
    finally:
        conn.close()


def _migrate_alert_columns(conn: sqlite3.Connection) -> None:
    """Add acknowledgement columns to a pre-workflow alerts table, once."""
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}
    for name, ddl in ALERT_COLUMN_MIGRATIONS:
        if name not in existing:
            conn.execute(ddl)
    # Created only after the columns above certainly exist, so an old alerts
    # table migrated in the same startup cannot fail the index build.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_device_status ON alerts(device_id, status)"
    )


@contextmanager
def write_txn(conn: sqlite3.Connection):
    """Exclusive write transaction; serializes writers at the SQLite level."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
