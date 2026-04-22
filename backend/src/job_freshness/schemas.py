from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# 类型枚举
# ---------------------------------------------------------------------------

TemporalStatus = Literal["has_signal", "no_signal", "cannot_determine", "conflict"]

SignalType = Literal[
    "absolute_datetime",
    "date_range",
    "relative_time",
    "duration_only",
    "holiday_window",
    "vague_time",
    "no_signal",
    "conflict",
]

AnchorType = Literal["publish_time", "message_time", "call_time", "unknown"]

ComplaintSignalType = Literal["full", "unreachable", "mixed", "other", "none"]

ErrorType = Literal[
    "parse_error",
    "schema_validation_error",
    "unknown_snippet_recall_error",
    "unknown_signal_detection_error",
    "unknown_time_normalization_error",
    "unknown_risk_assess_error",
    "unknown_final_decision_error",
]

# ---------------------------------------------------------------------------
# WideRow（职位宽表输入）
# ---------------------------------------------------------------------------


class WideRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    info_id: str
    job_detail: str = ""
    occupation_id: str = ""
    sub_id: str | None = None
    asr_result: str = ""
    im_text: str = ""
    complaint_content: str = ""
    im_message_count: int = Field(ge=0, default=0)
    call_record_count: int = Field(ge=0, default=0)
    complaint_count: int = Field(ge=0, default=0)
    publish_time: str | None = None  # 职位发布时间，可用于时间归一化锚点


# ---------------------------------------------------------------------------
# RiskContext（辅助上下文）
# ---------------------------------------------------------------------------


class RiskContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    complaint_text: str = ""
    im_message_count: int = Field(ge=0, default=0)
    call_record_count: int = Field(ge=0, default=0)
    complaint_count: int = Field(ge=0, default=0)


# ---------------------------------------------------------------------------
# Snippet Recall 相关模型
# ---------------------------------------------------------------------------


class RuleMatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["job_detail", "im_text", "asr_text"]
    matched_terms: list[str] = Field(default_factory=list)
    matched_bucket: str = ""  # absolute_time | recruitment_action | duration | deadline | holiday | complaint


class SnippetRecallRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    has_recall: bool = False
    matches: list[RuleMatch] = Field(default_factory=list)
    temporal_match_count: int = Field(ge=0, default=0)   # 非 complaint 桶的命中数
    complaint_match_count: int = Field(ge=0, default=0)   # complaint 桶的命中数
    matched_sources: list[str] = Field(default_factory=list)  # 有命中的来源: ["job_detail", "im_text"]


# ---------------------------------------------------------------------------
# 时效信号提取模型
# ---------------------------------------------------------------------------


class SignalDetectionRecord(BaseModel):
    """signal_detection 节点的直接输出（轻量）"""

    model_config = ConfigDict(extra="forbid")

    has_temporal_signal: bool = False
    temporal_status: TemporalStatus = "no_signal"
    signal_type: SignalType = "no_signal"
    evidence_summary: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    cannot_determine_reason: str | None = None


class TimeNormalizationRecord(BaseModel):
    """time_normalization 节点的直接输出（仅 has_signal=true 时产出）"""

    model_config = ConfigDict(extra="forbid")

    normalizable: bool = False
    anchor_type: AnchorType = "unknown"
    work_start_at: str | None = None
    recruitment_valid_until: str | None = None
    duration_hours: int | None = Field(default=None, ge=0)
    interpretation: str = ""
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)


class TemporalSignalRecord(BaseModel):
    """合并 detection + normalization 的完整记录，供 final_decision 和下游使用"""

    model_config = ConfigDict(extra="forbid")

    has_temporal_signal: bool = False
    temporal_status: TemporalStatus = "no_signal"
    signal_type: SignalType = "no_signal"
    normalizable: bool = False
    anchor_type: AnchorType = "unknown"
    work_start_at: str | None = None
    recruitment_valid_until: str | None = None
    duration_hours: int | None = Field(default=None, ge=0)
    evidence_summary: list[str] = Field(default_factory=list)
    interpretation: str = ""
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    cannot_determine_reason: str | None = None


# ---------------------------------------------------------------------------
# 风险评估模型
# ---------------------------------------------------------------------------


class ComplaintRiskHint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    has_complaint_signal: bool = False
    complaint_signal_type: ComplaintSignalType = "none"
    complaint_summary: str = ""


class RiskRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stale_risk_hint: bool = False
    complaint_risk_hint: ComplaintRiskHint = Field(default_factory=ComplaintRiskHint)
    risk_score: float = Field(ge=0.0, le=1.0, default=0.0)
    risk_reasons: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)


# ---------------------------------------------------------------------------
# 最终决策模型
# ---------------------------------------------------------------------------


class FreshnessDecisionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    temporal_status: TemporalStatus = "no_signal"
    signal_type: SignalType = "no_signal"
    work_start_at: str | None = None
    recruitment_valid_until: str | None = None
    duration_hours: int | None = Field(default=None, ge=0)
    normalizable: bool = False
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    stale_risk_hint: bool = False
    complaint_risk_hint: ComplaintRiskHint = Field(default_factory=ComplaintRiskHint)
    risk_score: float = Field(ge=0.0, le=1.0, default=0.0)
    risk_reasons: list[str] = Field(default_factory=list)
    evidence_summary: list[str] = Field(default_factory=list)
    decision_reason: str = ""
    low_confidence: bool = False
