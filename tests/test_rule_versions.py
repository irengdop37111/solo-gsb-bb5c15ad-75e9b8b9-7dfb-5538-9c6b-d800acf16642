"""Versioned alert-rule switching tests.

Covers: version append on modify, per-seq effective version selection on
both direct commits and buffered drains, independent per-version streak
state, immutable history, version listing, version-filtered alerts, SQLite
migration from the pre-versioning schema, and real-server concurrency:
parallel rule edits racing a large buffered -> committed drain must still
attribute every record seq to exactly one effective version.
"""
from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        window_size=10_000,
        gap_timeout_seconds=600,
        sweep_enabled=False,
    )
    with TestClient(create_app(settings)) as c:
        yield c


# --------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------- #

def submit(client, device, seqs, value=40.0, field="temp"):
    resp = client.post(
        "/api/v1/batches",
        json={
            "device_id": device,
            "records": [{"seq": s, "payload": {field: value}} for s in seqs],
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def submit_payloads(client, device, entries):
    resp = client.post(
        "/api/v1/batches",
        json={
            "device_id": device,
            "records": [{"seq": s, "payload": p} for s, p in entries],
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def create_rule(client, device="dev-a", field="temp", op="gt",
                threshold=30.0, consecutive=3):
    resp = client.post(
        f"/api/v1/devices/{device}/alert-rules",
        json={"field": field, "op": op, "threshold": threshold,
              "consecutive": consecutive},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def update_rule(client, rule_id, body, device="dev-a"):
    return client.put(
        f"/api/v1/devices/{device}/alert-rules/{rule_id}", json=body
    )


def versions(client, rule_id, device="dev-a"):
    resp = client.get(
        f"/api/v1/devices/{device}/alert-rules/{rule_id}/versions"
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["versions"]


def alerts(client, device="dev-a", **params):
    resp = client.get(f"/api/v1/devices/{device}/alerts", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()["alerts"]


def version_states(client, rule_id, device="dev-a"):
    return {
        v["version"]: v["state"]
        for v in versions(client, rule_id, device)
    }


# --------------------------------------------------------------------- #
# version lifecycle
# --------------------------------------------------------------------- #

def test_create_rule_starts_at_version_one_boundary_zero(client):
    rule = create_rule(client, consecutive=3)
    assert rule["version"] == 1
    assert rule["effective_from_seq"] == 0
    assert rule["state"] == {"last_seq": None, "streak": 0, "alerted": False}
    assert [(v["version"], v["effective_from_seq"]) for v in versions(client, rule["rule_id"])] == [
        (1, 0)
    ]


def test_update_appends_incrementing_version(client):
    rule = create_rule(client, consecutive=3)
    resp = update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 50.0,
        "consecutive": 2, "effective_from_seq": 10,
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["version"] == 2
    assert body["threshold"] == 50.0 and body["consecutive"] == 2
    assert body["effective_from_seq"] == 10
    # a fresh version starts cold
    assert body["state"] == {"last_seq": None, "streak": 0, "alerted": False}
    # rule-level fields mirror the latest version, and history is retained
    assert body["field"] == "temp"
    assert [(v["version"], v["threshold"], v["effective_from_seq"])
            for v in body["versions"]] == [(1, 30.0, 0), (2, 50.0, 10)]

    resp = update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "lt", "threshold": 1.0, "consecutive": 1,
        "effective_from_seq": 10,
    })
    # omitted boundary defaults to next_expected, which cannot regress; here
    # the boundary stays at the latest version's value and the version still
    # increments (a same-boundary replacement).
    assert resp.status_code == 200, resp.text
    assert resp.json()["version"] == 3
    assert resp.json()["effective_from_seq"] == 10
    assert resp.json()["op"] == "lt"


def test_default_boundary_tracks_next_expected(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2, 3, 4])  # next_expected -> 5
    resp = update_rule(client, 1, {
        "field": "temp", "op": "gt", "threshold": 99.0, "consecutive": 2,
    })
    assert resp.status_code == 200
    assert resp.json()["effective_from_seq"] == 5


def test_boundary_must_not_regress(client):
    rule = create_rule(client, consecutive=3)
    assert update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 50.0, "consecutive": 2,
        "effective_from_seq": 10,
    }).status_code == 200
    resp = update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 60.0, "consecutive": 2,
        "effective_from_seq": 9,
    })
    assert resp.status_code == 422
    assert "effective_from_seq" in resp.json()["detail"]
    # the rejected edit created no version
    assert [v["version"] for v in versions(client, rule["rule_id"])] == [1, 2]


def test_equal_boundary_allowed_and_newer_version_wins(client):
    # Two versions at the same boundary model "replace right now"; the higher
    # version number deterministically wins ties.
    rule = create_rule(client, consecutive=2)
    update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 30.0, "consecutive": 2,
        "effective_from_seq": 5,
    })
    update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 10.0, "consecutive": 2,
        "effective_from_seq": 5,
    })
    submit(client, "dev-a", [5, 6], value=20.0)  # violates v3 (20>10), not v2
    got = alerts(client)
    assert [(a["start_seq"], a["rule_version"], a["threshold"]) for a in got] == [
        (5, 3, 10.0)
    ]


def test_update_unknown_or_other_device_rule_404(client):
    assert update_rule(client, 999, {
        "field": "temp", "op": "gt", "threshold": 1.0, "consecutive": 1,
    }).status_code == 404
    rule = create_rule(client, device="dev-a", consecutive=1)
    assert update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 1.0, "consecutive": 1,
    }, device="dev-b").status_code == 404
    assert client.get(
        "/api/v1/devices/dev-b/alert-rules/%d/versions" % rule["rule_id"]
    ).status_code == 404


def test_update_request_validation(client):
    rule = create_rule(client, consecutive=3)
    path = f"/api/v1/devices/dev-a/alert-rules/{rule['rule_id']}"
    for bad in (
        {"field": "", "op": "gt", "threshold": 1.0, "consecutive": 1},
        {"field": "temp", "op": "gte", "threshold": 1.0, "consecutive": 1},
        {"field": "temp", "op": "gt", "threshold": 1.0, "consecutive": 0},
        {"field": "temp", "op": "gt", "threshold": 1.0, "consecutive": 1,
         "effective_from_seq": -1},
    ):
        assert client.put(path, json=bad).status_code == 422, bad
    resp = client.put(
        path,
        content='{"field":"temp","op":"gt","threshold":NaN,"consecutive":1}',
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 422
    assert [v["version"] for v in versions(client, rule["rule_id"])] == [1]


# --------------------------------------------------------------------- #
# per-seq effective version selection
# --------------------------------------------------------------------- #

def test_effective_version_chosen_per_committed_seq(client):
    rule = create_rule(client, consecutive=2)
    update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 50.0, "consecutive": 2,
        "effective_from_seq": 4,
    })
    # seqs 1..3 under v1 (40 > 30): alert (1,2) v1; v1 streak 3 after seq 3
    submit(client, "dev-a", [1, 2, 3])
    # seqs 4..6 under v2 (40 is NOT > 50): v2 streak stays 0, no v2 alert
    submit(client, "dev-a", [4, 5, 6])
    got = alerts(client)
    assert [(a["start_seq"], a["end_seq"], a["rule_version"]) for a in got] == [
        (1, 2, 1)
    ]
    states = version_states(client, rule["rule_id"])
    assert states[1] == {"last_seq": 3, "streak": 3, "alerted": True}
    assert states[2] == {"last_seq": 6, "streak": 0, "alerted": False}

    # seqs 7,8 violate v2 -> a version-2 alert
    submit(client, "dev-a", [7, 8], value=60.0)
    got = alerts(client)
    assert [(a["start_seq"], a["end_seq"], a["rule_version"], a["threshold"])
            for a in got] == [(1, 2, 1, 30.0), (7, 8, 2, 50.0)]


def test_buffered_drain_selects_version_per_record(client):
    # The drain of buffered records (buffered -> committed) must pick the
    # effective version for every individual seq, atomically in one txn.
    rule = create_rule(client, consecutive=2)
    update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 50.0, "consecutive": 2,
        "effective_from_seq": 5,
    })
    submit(client, "dev-a", [1])       # v1 streak 1
    submit(client, "dev-a", [3, 4, 5, 6, 7])  # buffered behind the hole at 2
    submit_payloads(client, "dev-a", [
        (2, {"temp": 40}),   # commits 2 then drains 3..7 in seq order
    ])
    # v1 evaluates 1..4  (40 > 30): alert (1,2); streak 4
    # v2 evaluates 5..7  (40 not > 50): no alert, streak 0
    got = alerts(client)
    assert [(a["start_seq"], a["end_seq"], a["rule_version"]) for a in got] == [
        (1, 2, 1)
    ]
    states = version_states(client, rule["rule_id"])
    assert states[1]["last_seq"] == 4 and states[1]["streak"] == 4
    assert states[2]["last_seq"] == 7 and states[2]["streak"] == 0


def test_switch_inside_drain_fires_each_version(client):
    # Boundary lands inside a buffered run; both versions fire on the drain.
    rule = create_rule(client, consecutive=2)
    update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 50.0, "consecutive": 2,
        "effective_from_seq": 4,
    })
    submit_payloads(client, "dev-a", [
        (s, {"temp": 60}) for s in (1, 3, 4, 5)
    ])  # 1 committed; 3..5 buffered (hole 2); all violate both thresholds
    submit_payloads(client, "dev-a", [
        (2, {"temp": 60}),  # drain: v1 sees 2,3 (60>30) completing its run;
                            # v2 sees 4,5  (60>50) firing a fresh version-2 run
    ])
    got = alerts(client)
    assert [(a["start_seq"], a["end_seq"], a["rule_version"]) for a in got] == [
        (1, 2, 1),  # seq 1 + drained seq 2 completed v1's run
        (4, 5, 2),
    ]
    states = version_states(client, rule["rule_id"])
    assert states[1]["last_seq"] == 3 and states[1]["streak"] == 3
    assert states[2]["last_seq"] == 5 and states[2]["streak"] == 2
    assert states[2]["alerted"] is True


def test_new_version_does_not_inherit_or_reset_previous_streak(client):
    rule = create_rule(client, consecutive=3)
    submit(client, "dev-a", [1, 2])  # v1 streak 2, unalerted
    update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 30.0, "consecutive": 3,
        "effective_from_seq": 3,
    })
    submit(client, "dev-a", [3])  # evaluated by v2 from a cold start
    states = version_states(client, rule["rule_id"])
    assert states[1] == {"last_seq": 2, "streak": 2, "alerted": False}
    assert states[2] == {"last_seq": 3, "streak": 1, "alerted": False}
    assert alerts(client) == []


def test_historical_alerts_immutable_after_modification(client):
    rule = create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])
    before = alerts(client)[0]
    update_rule(client, rule["rule_id"], {
        "field": "hum", "op": "lt", "threshold": 1.0, "consecutive": 1,
        "effective_from_seq": 3,
    })
    after = alerts(client)[0]
    # the old alert keeps its snapshot (field/op/threshold/consecutive) and
    # the producing version even though the rule has moved on
    assert after == before
    assert after["rule_version"] == 1
    assert after["field"] == "temp" and after["threshold"] == 30.0


def test_version_history_is_listed_per_rule(client):
    rule = create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])
    update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 50.0, "consecutive": 1,
        "effective_from_seq": 3,
    })
    vs = versions(client, rule["rule_id"])
    assert [v["version"] for v in vs] == [1, 2]
    assert vs[0]["field"] == "temp" and vs[0]["threshold"] == 30.0
    assert vs[0]["state"]["streak"] == 2
    assert vs[1]["threshold"] == 50.0 and vs[1]["consecutive"] == 1
    assert vs[1]["effective_from_seq"] == 3
    assert vs[1]["state"] == {"last_seq": None, "streak": 0, "alerted": False}


# --------------------------------------------------------------------- #
# version-filtered alert listing
# --------------------------------------------------------------------- #

def test_alerts_filter_by_version(client):
    rule = create_rule(client, consecutive=1)
    update_rule(client, rule["rule_id"], {
        "field": "temp", "op": "gt", "threshold": 50.0, "consecutive": 1,
        "effective_from_seq": 3,
    })
    # alternate violating / normal records so each violating seq is its own
    # run (alerts fire once per run, not once per violating record)
    submit_payloads(client, "dev-a", [
        (1, {"temp": 40}), (2, {"temp": 0}),
        (3, {"temp": 60}), (4, {"temp": 0}),
        (5, {"temp": 60}),
    ])
    all_alerts = alerts(client)
    assert [a["alert_id"] for a in all_alerts] == [1, 2, 3]
    assert [(a["end_seq"], a["rule_version"]) for a in all_alerts] == [
        (1, 1), (3, 2), (5, 2)
    ]

    assert [a["alert_id"] for a in alerts(client, rule_version=1)] == [1]
    assert [a["alert_id"] for a in alerts(client, rule_version=2)] == [2, 3]
    assert alerts(client, rule_version=99) == []

    # composes with rule_id and status
    client.post(
        "/api/v1/devices/dev-a/alerts/2/acknowledge",
        json={"acknowledged_by": "ops-alice"},
    )
    got = alerts(client, rule_id=rule["rule_id"], rule_version=2,
                 status="acknowledged")
    assert [a["alert_id"] for a in got] == [2]
    assert [a["alert_id"] for a in alerts(client, rule_version=2, status="open")] == [3]

    resp = client.get("/api/v1/devices/dev-a/alerts", params={"rule_version": 0})
    assert resp.status_code == 422


# --------------------------------------------------------------------- #
# declared gaps interact with versions
# --------------------------------------------------------------------- #

def test_declared_gap_resets_every_version(tmp_path):
    # needs the gap to expire lazily: use a short-timeout client
    settings = Settings(
        db_path=str(tmp_path / "gap.db"),
        window_size=10_000,
        gap_timeout_seconds=0.25,
        sweep_enabled=False,
    )
    with TestClient(create_app(settings)) as client:
        rule = create_rule(client, consecutive=3)
        update_rule(client, rule["rule_id"], {
            "field": "temp", "op": "gt", "threshold": 50.0, "consecutive": 3,
            "effective_from_seq": 10,
        })
        # an already-alerted run must be re-armed by the declared gap as well
        submit(client, "dev-a", [1, 2, 3])
        assert alerts(client) and version_states(client, 1)[1]["alerted"] is True
        submit(client, "dev-a", [7])        # buffered behind pending gap 4..6
        time.sleep(0.3)
        client.get("/api/v1/devices/dev-a/window")  # lazy gap declaration; drains 7
        states = version_states(client, rule["rule_id"])
        # The declared gap resets every version (alerted re-armed) BEFORE the
        # buffered drain, so seq 7 — below v2's boundary 10 — starts a fresh
        # v1 run instead of extending the old alerted one.
        assert states[1] == {"last_seq": 7, "streak": 1, "alerted": False}
        assert states[2] == {"last_seq": None, "streak": 0, "alerted": False}


# --------------------------------------------------------------------- #
# SQLite migration from the pre-versioning schema
# --------------------------------------------------------------------- #

def test_pre_versioning_database_is_migrated(tmp_path):
    import sqlite3

    from app.db import connect, init_db

    path = str(tmp_path / "old.db")
    with sqlite3.connect(path) as raw:
        raw.execute(
            "CREATE TABLE alert_rules (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "device_id TEXT, field TEXT, op TEXT, threshold REAL, "
            "consecutive INTEGER, created_at REAL)"
        )
        raw.execute(
            "CREATE TABLE alert_rule_state (rule_id INTEGER PRIMARY KEY, "
            "last_seq INTEGER, streak INTEGER, alerted INTEGER, updated_at REAL)"
        )
        raw.execute(
            "CREATE TABLE alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "rule_id INTEGER, device_id TEXT, field TEXT, op TEXT, "
            "threshold REAL, consecutive INTEGER, start_seq INTEGER, "
            "end_seq INTEGER, trigger_value REAL, created_at REAL, "
            "status TEXT DEFAULT 'open', acknowledged_by TEXT, "
            "acknowledged_at REAL)"
        )
        raw.execute(
            "INSERT INTO alert_rules (id, device_id, field, op, threshold, "
            "consecutive, created_at) VALUES (1, 'dev-a', 'temp', 'gt', 30, 2, 1.0)"
        )
        raw.execute(
            "INSERT INTO alert_rule_state (rule_id, last_seq, streak, alerted, "
            "updated_at) VALUES (1, 4, 2, 1, 2.0)"
        )
        raw.execute(
            "INSERT INTO alerts (rule_id, device_id, field, op, threshold, "
            "consecutive, start_seq, end_seq, trigger_value, created_at) "
            "VALUES (1, 'dev-a', 'temp', 'gt', 30, 2, 3, 4, 40, 2.0)"
        )

    init_db(path)  # rebuilds alert_rules, imports version 1 + state
    init_db(path)  # migration is idempotent

    conn = connect(path)
    try:
        rule = conn.execute("SELECT * FROM alert_rules WHERE id = 1").fetchone()
        assert rule["device_id"] == "dev-a" and rule["created_at"] == 1.0
        v1 = conn.execute(
            "SELECT * FROM alert_rule_versions WHERE rule_id = 1 AND version = 1"
        ).fetchone()
        assert (v1["field"], v1["op"], v1["threshold"], v1["consecutive"],
                v1["effective_from_seq"]) == ("temp", "gt", 30.0, 2, 0)
        st = conn.execute(
            "SELECT * FROM alert_rule_version_state WHERE rule_id = 1 AND version = 1"
        ).fetchone()
        assert (st["last_seq"], st["streak"], st["alerted"]) == (4, 2, 1)
        alert = conn.execute("SELECT * FROM alerts WHERE id = 1").fetchone()
        assert alert["rule_version"] == 1
        # AUTOINCREMENT counter survived the table rebuild
        cur = conn.execute(
            "INSERT INTO alert_rules (device_id, created_at) VALUES ('dev-b', 3.0)"
        )
        assert cur.lastrowid == 2
    finally:
        conn.close()

    # the migrated rule behaves normally through the API: modify appends v2
    settings = Settings(db_path=path, sweep_enabled=False)
    with TestClient(create_app(settings)) as c:
        resp = c.put(
            "/api/v1/devices/dev-a/alert-rules/1",
            json={"field": "temp", "op": "gt", "threshold": 50.0,
                  "consecutive": 1, "effective_from_seq": 100},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["version"] == 2
        old = c.get("/api/v1/devices/dev-a/alerts",
                    params={"rule_version": 1}).json()["alerts"]
        assert len(old) == 1 and old[0]["threshold"] == 30.0


def test_pre_versioning_database_without_state_table_is_migrated(tmp_path):
    """A legacy rules table whose deployment never created alert_rule_state
    (rule created but never used) must still migrate cleanly."""
    import sqlite3

    from app.db import connect, init_db

    path = str(tmp_path / "partial.db")
    with sqlite3.connect(path) as raw:
        raw.execute(
            "CREATE TABLE alert_rules (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "device_id TEXT, field TEXT, op TEXT, threshold REAL, "
            "consecutive INTEGER, created_at REAL)"
        )
        raw.execute(
            "INSERT INTO alert_rules (id, device_id, field, op, threshold, "
            "consecutive, created_at) VALUES (1, 'dev-a', 'temp', 'gt', 30, 2, 1.0)"
        )
        raw.execute(
            "CREATE TABLE alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "rule_id INTEGER, device_id TEXT, field TEXT, op TEXT, "
            "threshold REAL, consecutive INTEGER, start_seq INTEGER, "
            "end_seq INTEGER, trigger_value REAL, created_at REAL)"
        )

    init_db(path)
    conn = connect(path)
    try:
        v1 = conn.execute(
            "SELECT * FROM alert_rule_versions WHERE rule_id = 1 AND version = 1"
        ).fetchone()
        assert (v1["field"], v1["effective_from_seq"]) == ("temp", 0)
        st = conn.execute(
            "SELECT * FROM alert_rule_version_state WHERE rule_id = 1 AND version = 1"
        ).fetchone()
        assert st is None  # no legacy state to copy; engine lazily tolerates it
    finally:
        conn.close()

    # the migrated rule still creates v2 and evaluates new records
    settings = Settings(db_path=path, sweep_enabled=False)
    with TestClient(create_app(settings)) as c:
        r = c.put(
            "/api/v1/devices/dev-a/alert-rules/1",
            json={"field": "temp", "op": "gt", "threshold": 50.0,
                  "consecutive": 1, "effective_from_seq": 1},
        )
        assert r.status_code == 200, r.text
        submit_payloads(c, "dev-a", [(1, {"temp": 60})])
        got = c.get("/api/v1/devices/dev-a/alerts").json()["alerts"]
        assert len(got) == 1 and got[0]["rule_version"] == 2


# --------------------------------------------------------------------- #
# concurrency against a real server
# --------------------------------------------------------------------- #

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_server(tmp_path, db_name="conc_versions.db", gap_timeout=600):
    import uvicorn

    settings = Settings(
        db_path=str(tmp_path / db_name),
        window_size=10_000,
        gap_timeout_seconds=gap_timeout,
        sweep_enabled=False,
    )
    port = _free_port()
    config = uvicorn.Config(create_app(settings), host="127.0.0.1", port=port,
                            log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            httpx.get(f"{base}/health", timeout=0.2)
            return server, thread, base
        except Exception:
            time.sleep(0.05)
    pytest.fail("server did not start")


def test_concurrent_switches_racing_drain_attribute_each_seq_once(tmp_path):
    """8 rule edits with the SAME boundary race the single drain txn.

    Setup: seq 1 committed; seqs 3..400 buffered behind the hole at 2.
    Workers append versions 2..9 all effective from seq 100 while another
    request fills seq 2, which drains 3..400 inside ONE write transaction.

    The per-version streak state partitions the seq space exactly once:
    * if the drain beat every edit, version 1 owns streaks over all 400 seqs
      and every new version stayed cold;
    * otherwise version 1 stops at 99 and exactly ONE of versions 2..9 owns a
      contiguous run of 301 seqs (100..400) — the unique version selected
      atomically for every record at or beyond the shared boundary — while
      the other new versions were never evaluated.
    Each evaluated version fires its single run-alert at the right seq.
    """
    server, thread, base = _start_server(tmp_path, "race_drain.db")
    try:
        with httpx.Client(base_url=base, timeout=10) as c:
            r = c.post(
                "/api/v1/devices/dev-a/alert-rules",
                json={"field": "temp", "op": "gt", "threshold": 30.0,
                      "consecutive": 1},
            )
            assert r.status_code == 201, r.text
            # anchor at 1, then buffer 3..400 (hole at 2)
            r = c.post("/api/v1/batches", json={
                "device_id": "dev-a",
                "records": [{"seq": 1, "payload": {"temp": 40}}],
            })
            assert r.status_code == 200, r.text
            r = c.post("/api/v1/batches", json={
                "device_id": "dev-a",
                "records": [{"seq": s, "payload": {"temp": 40}}
                            for s in range(3, 401)],
            })
            assert r.status_code == 200, r.text
            win = c.get("/api/v1/devices/dev-a/window").json()
            assert win["next_expected"] == 2 and win["buffered_count"] == 398

        n_editors = 8
        barrier = threading.Barrier(n_editors + 1)

        def edit(worker: int):
            # thresholds differ but 40 violates all of them; only the version
            # number distinguishes winners.
            with httpx.Client(base_url=base, timeout=10) as c:
                barrier.wait()
                r = c.put(
                    "/api/v1/devices/dev-a/alert-rules/1",
                    json={"field": "temp", "op": "gt",
                          "threshold": float(worker + 1), "consecutive": 1,
                          "effective_from_seq": 100},
                )
                assert r.status_code == 200, r.text
                return r.json()["version"]

        def drain():
            with httpx.Client(base_url=base, timeout=10) as c:
                barrier.wait()
                r = c.post("/api/v1/batches", json={
                    "device_id": "dev-a",
                    "records": [{"seq": 2, "payload": {"temp": 40}}],
                })
                assert r.status_code == 200, r.text

        with ThreadPoolExecutor(max_workers=n_editors + 1) as pool:
            edit_futs = [pool.submit(edit, w) for w in range(n_editors)]
            drain_fut = pool.submit(drain)
            drain_fut.result()
            created_versions = sorted(f.result() for f in edit_futs)
        assert created_versions == list(range(2, 2 + n_editors))

        with httpx.Client(base_url=base, timeout=10) as c:
            win = c.get("/api/v1/devices/dev-a/window").json()
            assert win["next_expected"] == 401 and win["buffered_count"] == 0

            vs = c.get(
                "/api/v1/devices/dev-a/alert-rules/1/versions"
            ).json()["versions"]
            states = {v["version"]: v["state"] for v in vs}
            eval_versions = [v for v, st in states.items()
                             if st["last_seq"] is not None]

            # the evaluated seqs belong to exactly one version: the streak
            # counts partition 1..400 with no overlap and no gap
            assert sum(states[v]["streak"] for v in eval_versions) == 400
            assert all(states[v]["alerted"] for v in eval_versions)

            if eval_versions == [1]:
                # the drain beat every edit: v1 saw everything, new ones cold
                winner = 1
                assert states[1] == {"last_seq": 400, "streak": 400,
                                     "alerted": True}
                assert all(states[v]["last_seq"] is None
                           for v in range(2, 2 + n_editors))
            else:
                assert eval_versions[0] == 1
                assert states[1] == {"last_seq": 99, "streak": 99,
                                     "alerted": True}
                winners = eval_versions[1:]
                assert len(winners) == 1  # unique effective version >= boundary
                winner = winners[0]
                assert states[winner] == {"last_seq": 400, "streak": 301,
                                          "alerted": True}
                # every non-winning new version never evaluated a record
                assert all(states[v]["last_seq"] is None
                           for v in range(2, 2 + n_editors) if v != winner)

            # alerts: one run-alert per evaluated version, pinned to it
            got = c.get("/api/v1/devices/dev-a/alerts").json()["alerts"]
            by_version = {}
            for a in got:
                by_version.setdefault(a["rule_version"], []).append(
                    (a["start_seq"], a["end_seq"]))
            assert by_version[1] == [(1, 1)]  # fired already at setup
            if winner != 1:
                assert by_version[winner] == [(100, 100)]
                assert sorted(by_version) == [1, winner]
            else:
                assert sorted(by_version) == [1]
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def test_concurrent_switches_serialize_and_selection_matches_final_versions(
    tmp_path,
):
    """8 edits at DISTINCT boundaries race each other (no drain running):
    the non-regression rule serializes them into a monotonic chain (regressing
    losers get 422), and the subsequent drain attributes each seq to exactly
    the version predicted by the final, committed version set. Streak counts
    across versions partition the whole seq space with no overlap or gap."""
    server, thread, base = _start_server(tmp_path, "race_chain.db")
    try:
        with httpx.Client(base_url=base, timeout=10) as c:
            r = c.post(
                "/api/v1/devices/dev-a/alert-rules",
                json={"field": "temp", "op": "gt", "threshold": 30.0,
                      "consecutive": 1},
            )
            assert r.status_code == 201, r.text
            c.post("/api/v1/batches", json={
                "device_id": "dev-a",
                "records": [{"seq": 1, "payload": {"temp": 40}}],
            })
            c.post("/api/v1/batches", json={
                "device_id": "dev-a",
                "records": [{"seq": s, "payload": {"temp": 40}}
                            for s in range(3, 501)],
            })

        boundaries = [100 + 50 * w for w in range(8)]  # 100,150,...,450
        barrier = threading.Barrier(8)

        def edit(w: int):
            with httpx.Client(base_url=base, timeout=10) as c:
                barrier.wait()
                r = c.put(
                    "/api/v1/devices/dev-a/alert-rules/1",
                    json={"field": "temp", "op": "gt",
                          "threshold": 30.0, "consecutive": 1,
                          "effective_from_seq": boundaries[w]},
                )
                return r.status_code

        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(edit, range(8)))

        with httpx.Client(base_url=base, timeout=10) as c:
            vs = c.get(
                "/api/v1/devices/dev-a/alert-rules/1/versions"
            ).json()["versions"]
            chain = [(v["version"], v["effective_from_seq"]) for v in vs]
            accepted = chain[1:]
            # all accepted edits form a version-ordered, strictly increasing
            # boundary chain; rejected edits are the regressing losers
            bounds = [b for _, b in accepted]
            assert bounds == sorted(bounds)
            assert len(bounds) == len(set(bounds))
            assert outcomes.count(422) == 8 - len(accepted)
            assert outcomes.count(200) == len(accepted)

            # now drain 3..500; per-seq selection must match the final table
            r = c.post("/api/v1/batches", json={
                "device_id": "dev-a",
                "records": [{"seq": 2, "payload": {"temp": 40}}],
            })
            assert r.status_code == 200, r.text

            vs = c.get(
                "/api/v1/devices/dev-a/alert-rules/1/versions"
            ).json()["versions"]
            bounds_sorted = sorted((v["effective_from_seq"], v["version"])
                                   for v in vs)
            states = {v["version"]: v["state"] for v in vs}

            # expected streak: contiguous violating records each version owns.
            # v1 covers seq 1 (setup) + 2..(b_min-1) committed by the drain;
            # losers at 422 create no version, so ranges derive from the
            # actual accepted boundary chain.
            b_min = bounds_sorted[1][0]
            expected_streak = {bounds_sorted[0][1]: b_min - 1}
            for i in range(1, len(bounds_sorted)):
                b, ver = bounds_sorted[i]
                end = (bounds_sorted[i + 1][0] - 1
                       if i + 1 < len(bounds_sorted) else 500)
                expected_streak[ver] = end - b + 1
            for ver, streak in expected_streak.items():
                assert states[ver]["streak"] == streak, (
                    ver, states[ver]["streak"], streak)
                assert states[ver]["alerted"] is True
            # partition: the per-version owned seqs cover 1..500 exactly once
            assert sum(expected_streak.values()) == 500

            # one run-alert per version: v1 fired at setup (end 1); each
            # accepted later version fires when the drain reaches its boundary.
            got = c.get("/api/v1/devices/dev-a/alerts").json()["alerts"]
            assert sorted((a["end_seq"], a["rule_version"]) for a in got) == sorted(
                [(1, bounds_sorted[0][1])] +
                [(b, ver) for b, ver in bounds_sorted[1:]]
            )
            # and no version was created for a rejected edit
            assert all(1 <= a["rule_version"] <= len(bounds_sorted) for a in got)
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def test_concurrent_batches_and_boundary_switch_stay_consistent(tmp_path):
    """End-to-end interleaving with a deterministic two-phase race:
    phase A commits 1..100; the switch to v2 (boundary 101) then races the
    phase-B batches committing 101..200. Whatever wins, the per-version
    streaks partition the committed seq space exactly once along seq 101 and
    every alert honors the boundary."""
    server, thread, base = _start_server(tmp_path, "race_batches.db")
    try:
        with httpx.Client(base_url=base, timeout=10) as c:
            r = c.post(
                "/api/v1/devices/dev-conc/alert-rules",
                json={"field": "temp", "op": "gt", "threshold": 30.0,
                      "consecutive": 1},
            )
            assert r.status_code == 201, r.text

        total, threads, chunk = 200, 8, 25
        # 9 parties: 8 workers plus the main thread, which only proceeds to
        # release phase B once every worker finished phase A.
        phase_done = threading.Barrier(threads + 1)
        release_phase_b = threading.Event()

        def worker(shard: int):
            lo_a, hi_a = shard * chunk + 1, (shard + 1) * chunk + 1
            with httpx.Client(base_url=base, timeout=10) as c:
                # phase A: commit the seqs below the future boundary (1..100),
                # sharded across the 8 workers
                for first in range(lo_a, min(hi_a, 101), 5):
                    seqs = list(range(first, min(first + 5, min(hi_a, 101))))
                    r = c.post("/api/v1/batches", json={
                        "device_id": "dev-conc",
                        "records": [{"seq": s, "payload": {"temp": 40}}
                                    for s in seqs],
                    })
                    assert r.status_code == 200, r.text
                phase_done.wait()
                release_phase_b.wait()
                # phase B: commit seqs at/above the boundary (101..200)
                for first in range(max(lo_a, 101), hi_a, 5):
                    seqs = list(range(first, min(first + 5, hi_a)))
                    r = c.post("/api/v1/batches", json={
                        "device_id": "dev-conc",
                        "records": [{"seq": s, "payload": {"temp": 40}}
                                    for s in seqs],
                    })
                    assert r.status_code == 200, r.text

        pool = ThreadPoolExecutor(max_workers=threads)
        futs = [pool.submit(worker, s) for s in range(threads)]
        phase_done.wait()

        # Every phase-A record (1..100) is committed under v1 first; only
        # then is v2 (boundary 101) inserted, then phase B released. This
        # fixes the global attribution while still running phase B's sharded
        # batches concurrently with each other: per-record effective-version
        # selection (direct commits and buffered drains) is exercised under
        # an already-active v2. The earlier same-boundary-vs-drain test
        # covers the case where the edit and the drain race head-to-head.
        with httpx.Client(base_url=base, timeout=10) as c:
            r = c.put(
                "/api/v1/devices/dev-conc/alert-rules/1",
                json={"field": "temp", "op": "gt", "threshold": 20.0,
                      "consecutive": 1, "effective_from_seq": 101},
            )
            assert r.status_code == 200, r.text

        release_phase_b.set()
        for f in futs:
            f.result()
        pool.shutdown()

        with httpx.Client(base_url=base, timeout=10) as c:
            win = c.get("/api/v1/devices/dev-conc/window").json()
            assert win["next_expected"] == total + 1
            assert win["stats"]["committed"] == total

            vs = c.get(
                "/api/v1/devices/dev-conc/alert-rules/1/versions"
            ).json()["versions"]
            states = {v["version"]: v["state"] for v in vs}
            bounds = {v["version"]: v["effective_from_seq"] for v in vs}
            assert sorted(states) == [1, 2] and bounds[2] == 101

            got = c.get("/api/v1/devices/dev-conc/alerts").json()["alerts"]
            # v1 necessarily evaluated the 100 below-boundary records first
            assert states[1]["streak"] == 100
            assert states[1]["alerted"] is True
            assert states[2]["last_seq"] == total
            assert states[2]["streak"] == 100
            assert states[2]["alerted"] is True
            # streak partition covers every committed seq exactly once
            assert states[1]["streak"] + states[2]["streak"] == total
            # alerts are attributed by seq boundary, never by commit timing
            assert {a["rule_version"] for a in got} == {1, 2}
            for a in got:
                if a["rule_version"] == 1:
                    assert a["end_seq"] <= 100
                else:
                    assert a["end_seq"] >= 101
    finally:
        server.should_exit = True
        thread.join(timeout=5)
