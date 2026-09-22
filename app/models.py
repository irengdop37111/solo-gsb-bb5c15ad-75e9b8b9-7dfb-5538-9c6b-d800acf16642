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


class AlertRuleVersionOut(BaseModel):
    """One immutable version of a rule and its own evaluation state."""
    rule_id: int
    version: int
    field: str
    op: str
    threshold: float
    consecutive: int
    effective_from_seq: int  # first committed seq governed by this version
    created_at: float
    state: AlertRuleStateOut


class AlertRuleOut(BaseModel):
    rule_id: int
    device_id: str
    # Rule-level fields mirror the *latest* version; `versions` carries the
    # full history (configuration snapshot + per-version state).
    field: str
    op: str
    threshold: float
    consecutive: int
    version: int
    effective_from_seq: int
    created_at: float
    state: AlertRuleStateOut
    versions: list[AlertRuleVersionOut] = []


class AlertRuleUpdateIn(BaseModel):
    """Body for modifying a rule: appends a new immutable version."""
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
    effective_from_seq: Optional[int] = Field(
        default=None,
        ge=0,
        description="First committed seq governed by the new version. Omit to "
        "default to the device's current next_expected (from now on). Must not "
        "be lower than any previous version's boundary.",
    )


class AlertRuleVersionList(BaseModel):
    device_id: str
    rule_id: int
    versions: list[AlertRuleVersionOut]


class AlertRuleList(BaseModel):
    device_id: str
    rules: list[AlertRuleOut]


class AlertOut(BaseModel):
    alert_id: int
    rule_id: int
    # Version of the rule configuration that produced this alert; pinned for
    # the row's lifetime and usable as a list filter.
    rule_version: int
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
