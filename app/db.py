"""SQLite access layer.

One connection per request (created by the FastAPI dependency in main.py).
WAL mode + busy_timeout allow concurrent readers and serialized writers;
all mutations happen inside BEGIN IMMEDIATE transactions (see write_txn),
which keeps per-device window state consistent under concurrent requests.

Schema note — versioned alert rules
-----------------------------------
`alert_rules` holds only a rule's identity (device + creation time). Every
modification appends an immutable row to `alert_rule_versions` with a
monotonic `version` and an `effective_from_seq` boundary: a version governs
every *committed* record with seq >= its boundary until a newer version
takes over. Per-version evaluation state lives in
`alert_rule_version_state`, and every fired alert pins the producing
`rule_version`, so historical versions, streak state and existing alerts
are never rewritten when a rule is modified.
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

-- A rule is just an identity container; all configuration is versioned
-- (see alert_rule_versions). Modifying a rule never mutates an old version.
CREATE TABLE IF NOT EXISTS alert_rules (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id  TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alert_rules_device ON alert_rules(device_id);

CREATE TABLE IF NOT EXISTS alert_rule_versions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id             INTEGER NOT NULL REFERENCES alert_rules (id) ON DELETE CASCADE,
    version             INTEGER NOT NULL CHECK (version >= 1),
    field               TEXT NOT NULL,          -- top-level payload key to inspect
    op                  TEXT NOT NULL CHECK (op IN ('gt', 'lt')),
    threshold           REAL NOT NULL,
    consecutive         INTEGER NOT NULL CHECK (consecutive >= 1),
    -- First committed seq this version may evaluate; the effective version
    -- for a record at seq is the newest version whose boundary is <= seq
    -- (ties broken by the higher version number).
    effective_from_seq  INTEGER NOT NULL DEFAULT 0 CHECK (effective_from_seq >= 0),
    created_at          REAL NOT NULL,
    UNIQUE (rule_id, version)
);
CREATE INDEX IF NOT EXISTS idx_alert_rule_versions_rule
    ON alert_rule_versions(rule_id, version);

-- Streak state is per version: a switch starts with a fresh run instead of
-- carrying (or resetting) the previous version's consecutive count.
CREATE TABLE IF NOT EXISTS alert_rule_version_state (
    rule_id    INTEGER NOT NULL,
    version    INTEGER NOT NULL,
    last_seq   INTEGER,                     -- last committed seq evaluated (NULL = none yet)
    streak     INTEGER NOT NULL DEFAULT 0,  -- current run of consecutive violations
    alerted    INTEGER NOT NULL DEFAULT 0,  -- 1 while the current run has fired its alert
    updated_at REAL NOT NULL,
    PRIMARY KEY (rule_id, version),
    FOREIGN KEY (rule_id, version)
        REFERENCES alert_rule_versions (rule_id, version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS alerts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id       INTEGER NOT NULL REFERENCES alert_rules (id) ON DELETE CASCADE,
    -- Version of the rule that produced this alert; immutable once written,
    -- which lets alerts be filtered by rule version long after edits.
    rule_version  INTEGER NOT NULL DEFAULT 1,
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
-- idx_alerts_device_status / idx_alerts_device_version are created by
-- _migrate_alert_columns after every alerts column variant is ensured, so
-- they also build on pre-workflow / pre-versioning databases.
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
    # Versioning migration: pre-versioning rows were all produced by version 1.
    ("rule_version",
     "ALTER TABLE alerts ADD COLUMN rule_version INTEGER NOT NULL DEFAULT 1"),
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
        _migrate_legacy_rule_versions(conn)
        _migrate_alert_columns(conn)
    finally:
        conn.close()


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _migrate_legacy_rule_versions(conn: sqlite3.Connection) -> None:
    """Upgrade the pre-versioning schema to versioned rules, once.

    Old layout: alert_rules carried field/op/threshold/consecutive directly
    and a single alert_rule_state row per rule. Every existing rule becomes
    version 1 with effective_from_seq = 0, and its state row is copied to the
    per-version state table. Existing alerts keep rule_version = 1 (added by
    the column migration). The table rebuild intentionally runs with foreign
    keys off: it drops/replaces the parent table referenced by old children.
    """
    if "field" not in _table_columns(conn, "alert_rules"):
        return  # already the versioned layout (or a freshly created database)

    # alert_rule_state is present on real deployments but may be absent on a
    # partially-provisioned old database (e.g. rules table created but never
    # used); the state copy below is conditional on its existence.
    has_legacy_state = bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'alert_rule_state'"
        ).fetchone()
    )

    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.executescript(
            """
            CREATE TABLE alert_rules_new (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id  TEXT NOT NULL,
                created_at REAL NOT NULL
            );

            INSERT INTO alert_rules_new (id, device_id, created_at)
            SELECT id, device_id, created_at FROM alert_rules;

            -- Every pre-versioning rule is imported as its immutable version 1,
            -- effective from seq 0 (i.e. all of history).
            INSERT INTO alert_rule_versions
                (rule_id, version, field, op, threshold, consecutive,
                 effective_from_seq, created_at)
            SELECT id, 1, field, op, threshold, consecutive, 0, created_at
            FROM alert_rules;
            """
        )
        if has_legacy_state:
            conn.execute(
                "INSERT OR IGNORE INTO alert_rule_version_state "
                "(rule_id, version, last_seq, streak, alerted, updated_at) "
                "SELECT rule_id, 1, last_seq, streak, alerted, updated_at "
                "FROM alert_rule_state"
            )
            conn.execute("DROP TABLE alert_rule_state")
        conn.executescript(
            """
            DROP TABLE alert_rules;
            ALTER TABLE alert_rules_new RENAME TO alert_rules;

            CREATE INDEX IF NOT EXISTS idx_alert_rules_device
                ON alert_rules(device_id);
            """
        )
        # DROP TABLE discarded alert_rules' AUTOINCREMENT counter; restore it
        # so rebuilt ids never get reused.
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) AS m FROM alert_rules").fetchone()["m"]
        if max_id:
            conn.execute(
                "INSERT INTO sqlite_sequence (name, seq) VALUES ('alert_rules', ?)",
                (max_id,),
            )
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _migrate_alert_columns(conn: sqlite3.Connection) -> None:
    """Add post-release columns to an older alerts table, idempotently."""
    existing = _table_columns(conn, "alerts")
    for name, ddl in ALERT_COLUMN_MIGRATIONS:
        if name not in existing:
            conn.execute(ddl)
    # Created only after the columns above certainly exist, so an old alerts
    # table migrated in the same startup cannot fail the index builds.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_device_status ON alerts(device_id, status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_device_version ON alerts(device_id, rule_version)"
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
