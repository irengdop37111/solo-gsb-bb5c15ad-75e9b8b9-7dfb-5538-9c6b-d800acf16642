"""Request/response schemas for the HTTP API."""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class RecordIn(BaseModel):
    seq: int = Field(ge=0, description="Monotonically increasing per-device sequence number")
    payload: dict[str, Any] = Field(default_factory=dict, description="Telemetry payload")


class BatchIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    batch_id: Optional[str] = Field(
        default=None,
        description="Optional client-side batch identifier (echoed back; retries are "
        "safe because every sequence is idempotent on its own)",
    )
    records: list[RecordIn] = Field(min_length=1, max_length=10_000)


class RecordResult(BaseModel):
    seq: int
    status: str  # committed | buffered | duplicate | gap_filled | out_of_window | stale


class WindowSummary(BaseModel):
    next_expected: int
    buffered_count: int


class BatchResponse(BaseModel):
    device_id: str
    batch_id: Optional[str]
    accepted: int
    results: list[RecordResult]
    window: WindowSummary


class RecordOut(BaseModel):
    seq: int
    payload: dict[str, Any]
    status: str  # committed | buffered | gap_filled
    received_at: float


class RecordPage(BaseModel):
    device_id: str
    records: list[RecordOut]
    # Opaque, HMAC-signed keyset cursor; null when this is the last page.
    next_cursor: Optional[str] = None


class AlertRuleIn(BaseModel):
    field: str = Field(
        min_length=1, max_length=128,
        description="Top-level payload key holding the numeric measurement to watch",
    )
    op: Literal["gt", "lt"] = Field(
        description="Violation test: field value > threshold (gt) or < threshold (lt)",
    )
    # Finiteness is enforced in the endpoint (a pydantic-level rejection of a
    # NaN/Infinity literal would itself fail to serialize into a 422 body).
    threshold: float
    consecutive: int = Field(
        ge=1, le=10_000,
        description="How many consecutive committed records must violate before alerting",
    )


class AlertRuleStateOut(BaseModel):
    last_seq: Optional[int]  # last committed seq evaluated; null before any commit
    streak: int              # current run of consecutive violations
    alerted: bool            # true while the current run has fired its alert


class AlertRuleOut(BaseModel):
    rule_id: int
    device_id: str
    field: str
    op: str
    threshold: float
    consecutive: int
    created_at: float
    state: AlertRuleStateOut


class AlertRuleList(BaseModel):
    device_id: str
    rules: list[AlertRuleOut]


class AlertOut(BaseModel):
    alert_id: int
    rule_id: int
    device_id: str
    field: str
    op: str
    threshold: float
    consecutive: int
    start_seq: int       # first seq of the violating run
    end_seq: int         # seq at which the run reached the consecutive count
    trigger_value: float
    created_at: float
    # Acknowledgement workflow. New alerts are 'open'; the first confirmation
    # switches them to 'acknowledged' and freezes acknowledged_by/at. All
    # historical fields above remain present on every response.
    status: Literal["open", "acknowledged"]
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[float] = None


class AlertAckIn(BaseModel):
    acknowledged_by: str = Field(
        min_length=1, max_length=128,
        description="Identifier of the operator/system confirming the alert "
        "(user id, service account, etc.). Recorded only on the first "
        "confirmation; repeated calls are idempotent and ignore it.",
    )


class AlertList(BaseModel):
    device_id: str
    alerts: list[AlertOut]
