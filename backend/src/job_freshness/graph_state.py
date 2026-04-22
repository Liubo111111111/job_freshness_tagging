from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from job_freshness.schemas import (
    FreshnessDecisionRecord,
    RiskRecord,
    SignalDetectionRecord,
    SnippetRecallRecord,
    TemporalSignalRecord,
    TimeNormalizationRecord,
    WideRow,
)


class GraphState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    entity_key: str  # = info_id
    feature_schema_version: str
    graph_version: str
    prompt_version_detection: str
    prompt_version_normalization: str
    prompt_version_risk: str
    prompt_version_final: str
    model_version_detection: str
    model_version_normalization: str
    model_version_risk: str
    model_version_final: str
    wide_row: WideRow
    raw_wide_row: WideRow | None = None
    snippet_recall_record: SnippetRecallRecord | None = None
    signal_detection_record: SignalDetectionRecord | None = None
    time_normalization_record: TimeNormalizationRecord | None = None
    temporal_signal_record: TemporalSignalRecord | None = None
    risk_record: RiskRecord | None = None
    decision_record: FreshnessDecisionRecord | None = None
    route: str = "in_progress"
    error_type: str | None = None
    timing_ms: dict[str, float] | None = None
