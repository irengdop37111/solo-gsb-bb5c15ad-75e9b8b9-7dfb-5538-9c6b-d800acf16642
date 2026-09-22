"""Consecutive-threshold alerting on committed telemetry, with versioned rules.

Rules are device-scoped: each names a top-level numeric payload field, a
comparison (gt/lt), a threshold and a consecutive count N. The engine is
driven exclusively by the window engine's commit path: a rule advances only
when a record turns `committed` for the first time, in ascending seq order
(including buffered records drained by a single commit). Duplicates, stale
records and gap backfills (gap_filled) never touch rule state, so retried
batches cannot double-count and 断档补录 does not recompute history.

Rule versioning
---------------
A rule is an identity container (`alert_rules`); every modification appends
an immutable `alert_rule_versions` row carrying a monotonic version number
and an `effective_from_seq` boundary. While committing record seq S inside a
write transaction the engine atomically selects the unique effective
version:

    SELECT ... FROM alert_rule_versions
    WHERE rule_id = ? AND effective_from_seq <= S
    ORDER BY effective_from_seq DESC, version DESC LIMIT 1

Because this lookup and the per-version state update run inside the same
BEGIN IMMEDIATE transaction as the record's commit (direct commit and a
buffered -> committed drain alike), every committed seq is evaluated against
exactly one version, even when rule edits and batches race: the boundary is
chosen by the record's seq, not by whichever transaction happened to run
last. Versions with a boundary above S never see the record; historical
versions, their state and the alerts they produced are never rewritten.

A new version starts with a fresh streak (state is per version). A declared
gap (断档), a missing field or a non-numeric value breaks the consecutive
run: the streak resets and that version re-arms. When a run first reaches N
consecutive violations exactly one alert row is written, pinned to the
version that produced it; the same run never fires again — only after the
streak has been broken (恢复正常) can the next run trigger a new alert.

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


class RuleVersionError(ValueError):
    """Raised when a rule modification violates versioning constraints."""


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
        """Register a rule at version 1 (effective from seq 0); it evaluates
        records committed from now on (no backfill over already-committed
        history)."""
        now = time.time()
        cur = self.conn.execute(
            "INSERT INTO alert_rules (device_id, created_at) VALUES (?, ?)",
            (device_id, now),
        )
        rule_id = cur.lastrowid
        self.conn.execute(
            "INSERT INTO alert_rule_versions (rule_id, version, field, op, threshold, "
            "consecutive, effective_from_seq, created_at) VALUES (?, 1, ?, ?, ?, ?, 0, ?)",
            (rule_id, field, op, threshold, consecutive, now),
        )
        self._init_state(rule_id, 1, now)
        return self.get_rule(rule_id)

    def update_rule(
        self,
        device_id: str,
        rule_id: int,
        field: str,
        op: str,
        threshold: float,
        consecutive: int,
        effective_from_seq: Optional[int] = None,
    ) -> Optional[dict]:
        """Append a new immutable version to an existing rule.

        `effective_from_seq` is the first committed seq governed by the new
        version; when omitted it defaults to the device's current
        next_expected ("from now on"). It must not be lower than any earlier
        version's boundary. Returns None when the rule does not exist for
        the device.
        """
        rule = self.conn.execute(
            "SELECT * FROM alert_rules WHERE id = ? AND device_id = ?",
            (rule_id, device_id),
        ).fetchone()
        if rule is None:
            return None

        if effective_from_seq is None:
            dev = self.conn.execute(
                "SELECT next_expected FROM devices WHERE device_id = ?", (device_id,)
            ).fetchone()
            # The device may not exist yet (rules pre-provision devices);
            # then there is no history to protect and seq 0 is the boundary.
            effective_from_seq = dev["next_expected"] if dev is not None else 0

        latest = self.conn.execute(
            "SELECT version, effective_from_seq FROM alert_rule_versions "
            "WHERE rule_id = ? ORDER BY version DESC LIMIT 1",
            (rule_id,),
        ).fetchone()
        if latest is not None and effective_from_seq < latest["effective_from_seq"]:
            raise RuleVersionError(
                f"effective_from_seq must be >= {latest['effective_from_seq']} "
                "(the boundary of the current latest version)"
            )

        now = time.time()
        new_version = (latest["version"] if latest is not None else 0) + 1
        self.conn.execute(
            "INSERT INTO alert_rule_versions (rule_id, version, field, op, threshold, "
            "consecutive, effective_from_seq, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rule_id, new_version, field, op, threshold, consecutive,
             effective_from_seq, now),
        )
        # A switched version starts cold: the previous version's streak is
        # neither carried over nor destroyed.
        self._init_state(rule_id, new_version, now)
        return self.get_rule(rule_id)

    def _init_state(self, rule_id: int, version: int, now: float) -> None:
        self.conn.execute(
            "INSERT INTO alert_rule_version_state (rule_id, version, last_seq, streak, "
            "alerted, updated_at) VALUES (?, ?, NULL, 0, 0, ?)",
            (rule_id, version, now),
        )

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

    def list_versions(self, device_id: str, rule_id: int) -> Optional[list[dict]]:
        """Return all versions of a device's rule, oldest first; None when
        the rule does not belong to the device."""
        owned = self.conn.execute(
            "SELECT 1 FROM alert_rules WHERE id = ? AND device_id = ?",
            (rule_id, device_id),
        ).fetchone()
        if owned is None:
            return None
        rows = self.conn.execute(
            "SELECT * FROM alert_rule_versions WHERE rule_id = ? ORDER BY version",
            (rule_id,),
        ).fetchall()
        return [self._version_dict(r) for r in rows]

    def _rule_dict(self, row) -> dict:
        versions = [
            self._version_dict(r)
            for r in self.conn.execute(
                "SELECT * FROM alert_rule_versions WHERE rule_id = ? ORDER BY version",
                (row["id"],),
            ).fetchall()
        ]
        latest = versions[-1]
        return {
            "rule_id": row["id"],
            "device_id": row["device_id"],
            "created_at": row["created_at"],
            # Latest configuration kept at the rule level for convenience.
            "field": latest["field"],
            "op": latest["op"],
            "threshold": latest["threshold"],
            "consecutive": latest["consecutive"],
            "version": latest["version"],
            "effective_from_seq": latest["effective_from_seq"],
            "state": latest["state"],
            "versions": versions,
        }

    def _version_dict(self, row) -> dict:
        state = self.conn.execute(
            "SELECT last_seq, streak, alerted FROM alert_rule_version_state "
            "WHERE rule_id = ? AND version = ?",
            (row["rule_id"], row["version"]),
        ).fetchone()
        return {
            "rule_id": row["rule_id"],
            "version": row["version"],
            "field": row["field"],
            "op": row["op"],
            "threshold": row["threshold"],
            "consecutive": row["consecutive"],
            "effective_from_seq": row["effective_from_seq"],
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
        rule_version: Optional[int] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        clauses = ["device_id = ?"]
        params: list = [device_id]
        if rule_id is not None:
            clauses.append("rule_id = ?")
            params.append(rule_id)
        if rule_version is not None:
            clauses.append("rule_version = ?")
            params.append(rule_version)
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
        self.conn.execute(
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
            "rule_version": row["rule_version"],
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
        """Advance, per rule, the unique version effective at this seq."""
        rules = self.conn.execute(
            "SELECT id FROM alert_rules WHERE device_id = ? ORDER BY id", (device_id,)
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
            version_row = self._effective_version(rule["id"], seq)
            if version_row is None:
                # All versions of this rule start above this seq; the record
                # is simply not evaluated by any of them.
                continue
            raw = payload.get(version_row["field"]) if payload is not None else None
            self._advance_version(device_id, version_row, seq, _as_number(raw), now)

    def on_gap_declared(self, device_id: str, now: float) -> None:
        """A declared 断档 breaks every run of every version; the hole is
        never recomputed. Per-version state: all versions of the device's
        rules reset (older versions only matter if a late drain still feeds
        them, which keeps the semantics uniform)."""
        self.conn.execute(
            "UPDATE alert_rule_version_state SET streak = 0, alerted = 0, updated_at = ? "
            "WHERE rule_id IN (SELECT id FROM alert_rules WHERE device_id = ?)",
            (now, device_id),
        )

    def _effective_version(self, rule_id: int, seq: int):
        """Select the unique version governing committed seq.

        Boundaries are seq positions: the newest version whose
        effective_from_seq <= seq wins (higher version breaks a tie). The
        SELECT runs in the caller's exclusive write transaction, so a
        concurrent rule edit can never make two versions observe the same
        record, nor make a record skip evaluation.
        """
        return self.conn.execute(
            "SELECT * FROM alert_rule_versions WHERE rule_id = ? AND effective_from_seq <= ? "
            "ORDER BY effective_from_seq DESC, version DESC LIMIT 1",
            (rule_id, seq),
        ).fetchone()

    def _advance_version(self, device_id: str, version_row, seq: int,
                         value: Optional[float], now: float) -> None:
        rule_id, ver = version_row["rule_id"], version_row["version"]
        state = self.conn.execute(
            "SELECT streak, alerted FROM alert_rule_version_state "
            "WHERE rule_id = ? AND version = ?",
            (rule_id, ver),
        ).fetchone()
        streak = state["streak"] if state is not None else 0
        alerted = bool(state["alerted"]) if state is not None else False

        violates = value is not None and (
            value > version_row["threshold"]
            if version_row["op"] == GT
            else value < version_row["threshold"]
        )
        if violates:
            streak += 1
            if streak >= version_row["consecutive"] and not alerted:
                self._fire(device_id, version_row, seq, streak, value, now)
                alerted = True
        else:
            # Normal value, missing field or non-numeric value: the run
            # breaks and this version re-arms for the next one.
            streak, alerted = 0, False

        self.conn.execute(
            "UPDATE alert_rule_version_state SET last_seq = ?, streak = ?, alerted = ?, "
            "updated_at = ? WHERE rule_id = ? AND version = ?",
            (seq, streak, int(alerted), now, rule_id, ver),
        )

    def _fire(self, device_id: str, version_row, seq: int, streak: int,
              value: float, now: float) -> None:
        self.conn.execute(
            "INSERT INTO alerts (rule_id, rule_version, device_id, field, op, threshold, "
            "consecutive, start_seq, end_seq, trigger_value, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                version_row["rule_id"], version_row["version"], device_id,
                version_row["field"], version_row["op"],
                version_row["threshold"], version_row["consecutive"],
                seq - streak + 1,  # first seq of the violating run
                seq,               # record that reached the consecutive count
                value, now,
            ),
        )
