"""Consecutive-threshold alerting on committed telemetry.

Rules are device-scoped: each names a top-level numeric payload field, a
comparison (gt/lt), a threshold and a consecutive count N. The engine is
driven exclusively by the window engine's commit path: a rule advances only
when a record turns `committed` for the first time, in ascending seq order
(including buffered records drained by a single commit). Duplicates, stale
records and gap backfills (gap_filled) never touch rule state, so retried
batches cannot double-count and 断档补录 does not recompute history.

A declared gap (断档), a missing field or a non-numeric value breaks the
consecutive run: the streak resets and the rule re-arms. When a run first
reaches N consecutive violations exactly one alert row is written; the same
run never fires again — only after the streak has been broken (恢复正常) can
the next run trigger a new alert.

All methods run inside the caller's write transaction (db.write_txn), so
alert state commits atomically with the record transitions that produced it.
"""
from __future__ import annotations

import json
import math
import time
from typing import Optional

# Comparison operators supported by alert rules.
GT = "gt"
LT = "lt"
OPERATORS = (GT, LT)


def _as_number(value) -> Optional[float]:
    """Return `value` as a finite float, or None if it is not a real number.

    Booleans are excluded (they are ints in Python but not measurements) and
    NaN/Infinity can never satisfy a threshold comparison.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


class AlertEngine:
    def __init__(self, conn):
        self.conn = conn

    # ------------------------------------------------------------------ #
    # rule management
    # ------------------------------------------------------------------ #
    def create_rule(self, device_id: str, field: str, op: str,
                    threshold: float, consecutive: int) -> dict:
        """Register a rule; it evaluates records committed from now on (no
        backfill over already-committed history)."""
        now = time.time()
        cur = self.conn.execute(
            "INSERT INTO alert_rules (device_id, field, op, threshold, consecutive, "
            "created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (device_id, field, op, threshold, consecutive, now),
        )
        self.conn.execute(
            "INSERT INTO alert_rule_state (rule_id, last_seq, streak, alerted, updated_at) "
            "VALUES (?, NULL, 0, 0, ?)",
            (cur.lastrowid, now),
        )
        return self.get_rule(cur.lastrowid)

    def get_rule(self, rule_id: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM alert_rules WHERE id = ?", (rule_id,)
        ).fetchone()
        return self._rule_dict(row) if row is not None else None

    def list_rules(self, device_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM alert_rules WHERE device_id = ? ORDER BY id", (device_id,)
        ).fetchall()
        return [self._rule_dict(r) for r in rows]

    def _rule_dict(self, row) -> dict:
        state = self.conn.execute(
            "SELECT last_seq, streak, alerted FROM alert_rule_state WHERE rule_id = ?",
            (row["id"],),
        ).fetchone()
        return {
            "rule_id": row["id"],
            "device_id": row["device_id"],
            "field": row["field"],
            "op": row["op"],
            "threshold": row["threshold"],
            "consecutive": row["consecutive"],
            "created_at": row["created_at"],
            "state": {
                "last_seq": state["last_seq"] if state is not None else None,
                "streak": state["streak"] if state is not None else 0,
                "alerted": bool(state["alerted"]) if state is not None else False,
            },
        }

    # ------------------------------------------------------------------ #
    # alert queries
    # ------------------------------------------------------------------ #
    def list_alerts(
        self,
        device_id: str,
        rule_id: Optional[int] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        clauses = ["device_id = ?"]
        params: list = [device_id]
        if rule_id is not None:
            clauses.append("rule_id = ?")
            params.append(rule_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        rows = self.conn.execute(
            f"SELECT * FROM alerts WHERE {' AND '.join(clauses)} ORDER BY id", params
        ).fetchall()
        return [self._alert_dict(r) for r in rows]

    def get_alert(self, device_id: str, alert_id: int) -> Optional[dict]:
        """Fetch one device-scoped alert, or None when it does not exist."""
        row = self.conn.execute(
            "SELECT * FROM alerts WHERE device_id = ? AND id = ?",
            (device_id, alert_id),
        ).fetchone()
        return self._alert_dict(row) if row is not None else None

    def acknowledge_alert(
        self, device_id: str, alert_id: int, acknowledged_by: str, now: float
    ) -> Optional[dict]:
        """Idempotently confirm an alert.

        Runs inside the caller's BEGIN IMMEDIATE transaction: the conditional
        UPDATE claims the alert only while it is still 'open', so concurrent
        confirmations serialize on the row and exactly one wins. A repeated
        call matches no row and simply returns the stored record — the first
        confirmer's identity and timestamp can never be rewritten.
        """
        cur = self.conn.execute(
            "UPDATE alerts SET status = 'acknowledged', acknowledged_by = ?, "
            "acknowledged_at = ? WHERE device_id = ? AND id = ? AND status = 'open'",
            (acknowledged_by, now, device_id, alert_id),
        )
        # When cur.rowcount == 0 the alert either does not exist (handled by
        # the following lookup -> None) or was already acknowledged (the
        # existing row is returned untouched).
        return self.get_alert(device_id, alert_id)

    @staticmethod
    def _alert_dict(row) -> dict:
        return {
            "alert_id": row["id"],
            "rule_id": row["rule_id"],
            "device_id": row["device_id"],
            "field": row["field"],
            "op": row["op"],
            "threshold": row["threshold"],
            "consecutive": row["consecutive"],
            "start_seq": row["start_seq"],
            "end_seq": row["end_seq"],
            "trigger_value": row["trigger_value"],
            "created_at": row["created_at"],
            "status": row["status"],
            "acknowledged_by": row["acknowledged_by"],
            "acknowledged_at": row["acknowledged_at"],
        }

    # ------------------------------------------------------------------ #
    # window-engine hooks (must run inside the caller's write transaction)
    # ------------------------------------------------------------------ #
    def on_committed(self, device_id: str, seq: int, payload_json: str, now: float) -> None:
        """Advance every rule of the device with one newly committed record."""
        rules = self.conn.execute(
            "SELECT * FROM alert_rules WHERE device_id = ? ORDER BY id", (device_id,)
        ).fetchall()
        if not rules:
            return
        try:
            payload = json.loads(payload_json)
        except ValueError:
            payload = None
        if not isinstance(payload, dict):
            payload = None
        for rule in rules:
            raw = payload.get(rule["field"]) if payload is not None else None
            self._advance_rule(rule, seq, _as_number(raw), now)

    def on_gap_declared(self, device_id: str, now: float) -> None:
        """A declared 断档 breaks every run; the hole is never recomputed."""
        self.conn.execute(
            "UPDATE alert_rule_state SET streak = 0, alerted = 0, updated_at = ? "
            "WHERE rule_id IN (SELECT id FROM alert_rules WHERE device_id = ?)",
            (now, device_id),
        )

    def _advance_rule(self, rule, seq: int, value: Optional[float], now: float) -> None:
        state = self.conn.execute(
            "SELECT streak, alerted FROM alert_rule_state WHERE rule_id = ?",
            (rule["id"],),
        ).fetchone()
        streak = state["streak"] if state is not None else 0
        alerted = bool(state["alerted"]) if state is not None else False

        violates = value is not None and (
            value > rule["threshold"] if rule["op"] == GT else value < rule["threshold"]
        )
        if violates:
            streak += 1
            if streak >= rule["consecutive"] and not alerted:
                self._fire(rule, seq, streak, value, now)
                alerted = True
        else:
            # Normal value, missing field or non-numeric value: the run
            # breaks and the rule re-arms for the next one.
            streak, alerted = 0, False

        self.conn.execute(
            "UPDATE alert_rule_state SET last_seq = ?, streak = ?, alerted = ?, "
            "updated_at = ? WHERE rule_id = ?",
            (seq, streak, int(alerted), now, rule["id"]),
        )

    def _fire(self, rule, seq: int, streak: int, value: float, now: float) -> None:
        self.conn.execute(
            "INSERT INTO alerts (rule_id, device_id, field, op, threshold, consecutive, "
            "start_seq, end_seq, trigger_value, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rule["id"], rule["device_id"], rule["field"], rule["op"],
                rule["threshold"], rule["consecutive"],
                seq - streak + 1,  # first seq of the violating run
                seq,               # record that reached the consecutive count
                value, now,
            ),
        )
