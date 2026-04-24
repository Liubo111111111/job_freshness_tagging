from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from job_freshness.schemas import (
    ComplaintRiskHint,
    FreshnessDecisionRecord,
    RiskRecord,
    SignalDetectionRecord,
    SnippetRecallRecord,
    TemporalSignalRecord,
    TimeNormalizationRecord,
    ValidityType,
)

# ---------------------------------------------------------------------------
# 日期分区
# ---------------------------------------------------------------------------


class DateEntry(BaseModel):
    pt: str
    record_count: int


class DatesResponse(BaseModel):
    dates: list[DateEntry]
    latest_pt: str | None


class DailySummaryEntry(BaseModel):
    pt: str
    total_count: int
    formal_count: int
    fallback_count: int


class DailySummaryResponse(BaseModel):
    summaries: list[DailySummaryEntry]


# ---------------------------------------------------------------------------
# 统计（新鲜度）
# ---------------------------------------------------------------------------


class StatsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    validity_type_distribution: dict[str, int] = Field(default_factory=dict)
    total_count: int = 0
    formal_count: int = 0
    fallback_count: int = 0


# ---------------------------------------------------------------------------
# 运行记录（新鲜度）
# ---------------------------------------------------------------------------


class RunSummary(BaseModel):
    """审核列表项：/api/runs 返回的摘要"""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    entity_key: str  # = info_id
    validity_type: ValidityType | None = None
    estimated_expiry: str | None = None
    stale_risk_hint: bool | None = None
    complaint_risk_hint: ComplaintRiskHint | None = None
    route: str  # formal / fallback
    error_type: str | None = None
    timestamp: str | None = None
    annotated_label: ValidityType | None = None
    annotations: list["AnnotationRecord"] = Field(default_factory=list)


class PaginatedRunList(BaseModel):
    items: list[RunSummary]
    total: int
    offset: int
    limit: int


class RunDetail(BaseModel):
    """单条运行记录完整详情：/api/runs/{run_id}"""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    entity_key: str
    wide_row: dict[str, Any]
    raw_wide_row: dict[str, Any] | None = None
    snippet_recall_record: dict[str, Any] | None = None
    signal_detection_record: dict[str, Any] | None = None
    time_normalization_record: dict[str, Any] | None = None
    temporal_signal_record: dict[str, Any] | None = None
    risk_record: dict[str, Any] | None = None
    decision_record: dict[str, Any] | None = None
    route: str
    error_type: str | None = None
    audit: dict[str, Any] = Field(default_factory=dict)
    timing_ms: dict[str, float] | None = None
    annotations: list["AnnotationRecord"] = Field(default_factory=list)


class AnnotationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    entity_key: str
    annotated_label: ValidityType
    reviewer_notes: str = ""
    reviewer_name: str = ""
    created_at: str


class AnnotationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    annotated_label: ValidityType
    reviewer_notes: str = ""
    reviewer_name: str = ""


class AnnotationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    annotated_label: ValidityType
    status: str = "annotated"
    annotation_count: int = 1


# ---------------------------------------------------------------------------
# 搜索
# ---------------------------------------------------------------------------


class SearchResult(BaseModel):
    entity_key: str  # = info_id
    validity_type: str | None = None
    estimated_expiry: str | None = None
    route: str
    run_id: str


# ---------------------------------------------------------------------------
# 批量任务
# ---------------------------------------------------------------------------


class BatchRequest(BaseModel):
    pt: str  # 业务日期 yyyymmdd
    input_path: str
    worker_count: int = Field(default=4, ge=1, le=32)
    provider_rate_limit_per_minute: int = Field(default=120, ge=1)
    max_in_flight: int = Field(default=8, ge=1)


class BatchAccepted(BaseModel):
    task_id: str
    message: str
    status: str = "accepted"


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


class AuthUserResponse(BaseModel):
    open_id: str
    name: str = ""
    en_name: str = ""
    avatar_url: str = ""
    email: str = ""
    enterprise_email: str = ""
    user_id: str = ""
    tenant_key: str = ""
    is_admin: bool = False


class AuthSessionResponse(BaseModel):
    enabled: bool
    authenticated: bool
    access_denied: bool = False
    request_status: str | None = None
    user: AuthUserResponse | None = None
    login_url: str | None = None


class AdminOverviewResponse(BaseModel):
    auth_enabled: bool
    admin_mode: str
    access_scope: str
    frontend_base_url: str
    redirect_uri: str
    host_consistent: bool
    allowed_open_id_count: int
    allowed_email_count: int
    admin_open_id_count: int
    admin_email_count: int
    warnings: list[str] = Field(default_factory=list)


class AccessSettingsResponse(BaseModel):
    allowed_open_ids: list[str] = Field(default_factory=list)
    allowed_emails: list[str] = Field(default_factory=list)
    admin_open_ids: list[str] = Field(default_factory=list)
    admin_emails: list[str] = Field(default_factory=list)


class AccessSettingsUpdate(BaseModel):
    allowed_open_ids: list[str] = Field(default_factory=list)
    allowed_emails: list[str] = Field(default_factory=list)
    admin_open_ids: list[str] = Field(default_factory=list)
    admin_emails: list[str] = Field(default_factory=list)


class AdminAuthAuditEventResponse(BaseModel):
    event_type: str
    open_id: str
    name: str = ""
    email: str = ""
    enterprise_email: str = ""
    user_id: str = ""
    tenant_key: str = ""
    is_admin: bool = False
    created_at: str


class AdminAuthAuditUserResponse(BaseModel):
    open_id: str
    name: str = ""
    email: str = ""
    enterprise_email: str = ""
    user_id: str = ""
    tenant_key: str = ""
    is_admin: bool = False
    last_event_type: str
    last_event_at: str
    event_count: int


class AdminAuthAuditResponse(BaseModel):
    events: list[AdminAuthAuditEventResponse] = Field(default_factory=list)
    users: list[AdminAuthAuditUserResponse] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# 系统配置
# ---------------------------------------------------------------------------


class SettingsResponse(BaseModel):
    llm_model: str
    llm_timeout_sec: int
    llm_max_retry: int
    worker_count: int
    provider_rate_limit_per_minute: int
    max_in_flight: int
    batch_max_rows: int
    fetch_only_filled_complaints: bool = False


class SettingsUpdate(BaseModel):
    llm_model: str | None = None
    llm_timeout_sec: int | None = Field(default=None, ge=1)
    llm_max_retry: int | None = Field(default=None, ge=0)
    worker_count: int | None = Field(default=None, ge=1, le=32)
    provider_rate_limit_per_minute: int | None = Field(default=None, ge=1)
    max_in_flight: int | None = Field(default=None, ge=1)
    batch_max_rows: int | None = Field(default=None, ge=1, le=100)
    fetch_only_filled_complaints: bool | None = None


# ---------------------------------------------------------------------------
# 任务状态追踪
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 在线查询
# ---------------------------------------------------------------------------


class OnlineQueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    info_ids: list[str]
    pt: str


class OnlineQueryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    results: list[RunDetail]
    not_found: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# 任务状态追踪
# ---------------------------------------------------------------------------


class TaskStageStatus(BaseModel):
    name: str
    status: str = "pending"  # pending | running | done | error
    elapsed_ms: float | None = None
    message: str = ""


class TaskStatus(BaseModel):
    task_id: str
    status: str = "pending"  # pending | running | done | error
    stages: list[TaskStageStatus] = Field(default_factory=list)
    result_run_id: str | None = None
    error: str | None = None
