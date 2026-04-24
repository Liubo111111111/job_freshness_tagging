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
FillStatus = Literal["confirmed_filled", "suspected_filled", "not_filled"]

ValidityType = Literal[
    "exact_date",       # 明确日期
    "fuzzy_time",       # 模糊时间
    "no_validity",      # 无时效
]

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
    first_complaint_time: str | None = None  # 最早一次投诉时间
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
    """投诉分析结果（纯规则，不调用 LLM）"""
    model_config = ConfigDict(extra="forbid")

    is_filled: bool = False  # 是否已招满
    fill_status: FillStatus = "not_filled"  # 三态招满状态
    is_unreachable: bool = False  # 是否联系不上
    complaint_summary: str = ""  # 投诉摘要
    estimated_filled_at: str | None = None  # 预估招满时间（从投诉时间戳推断）
    estimated_filled_reason: str = ""  # 推断依据
    # 风险评估扩展字段
    risk_score: float = Field(ge=0.0, le=1.0, default=0.0)  # 风险分数
    risk_reasons: list[str] = Field(default_factory=list)  # 风险原因列表
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)  # 置信度
    stale_risk_hint: bool = False  # 过期风险提示
    complaint_risk_hint: ComplaintRiskHint | None = None  # 投诉风险提示


# ---------------------------------------------------------------------------
# 最终决策模型
# ---------------------------------------------------------------------------


class FreshnessDecisionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    validity_type: ValidityType = "no_validity"  # 有效期类型
    estimated_expiry: str | None = None  # 预估有效期截止时间（ISO8601）
    reason: str = ""  # 决策理由
    low_confidence: bool = False  # 置信度低标记（用于路由判断）
