"""TimeNormalizationService — 条件执行：仅当 has_temporal_signal=True 时调用 LLM 归一化时间。"""

from __future__ import annotations

import logging
import time
from typing import Any

from job_freshness.graph_state import GraphState
from job_freshness.nodes.time_normalization.parser import (
    parse_time_normalization,
)
from job_freshness.nodes.time_normalization.prompt_builder import (
    build_time_normalization_prompt,
)
from job_freshness.schemas import TemporalSignalRecord

logger = logging.getLogger(__name__)


class TimeNormalizationService:
    """条件执行：仅当 signal_detection_record.has_temporal_signal=True 时调用 LLM。

    从检测结果和原始文本归一化时间字段，合并 detection + normalization
    为完整的 TemporalSignalRecord 写入 GraphState。
    """

    def __init__(
        self,
        client: Any,
        model_version: str,
        prompt_version: str,
        sqlite_store: Any | None = None,
    ) -> None:
        self._client = client
        self._model_version = model_version
        self._prompt_version = prompt_version
        self._sqlite_store = sqlite_store

    def run(self, state: GraphState) -> GraphState:
        """条件路由 → 构建 prompt → 调用 LLM → 解析 → 合并 → 更新 GraphState。"""
        t0 = time.perf_counter()
        detection = state.signal_detection_record

        # ── 无信号：跳过 LLM，直接从 SignalDetectionRecord 构造默认 TemporalSignalRecord ──
        if detection is None or not detection.has_temporal_signal:
            temporal = _build_skip_record(detection)
            logger.info(
                "time_normalization_skipped job_id=%s has_signal=False",
                state.wide_row.info_id,
            )
            return state.model_copy(
                update={
                    "temporal_signal_record": temporal,
                    "timing_ms": _update_timing(state, t0),
                }
            )

        # ── 有信号：调用 LLM 归一化 ──
        try:
            prompt = build_time_normalization_prompt(state, self._prompt_version)
            raw_text = self._client.complete(prompt, {})
            norm_record, error_type = parse_time_normalization(raw_text)

            if error_type is not None:
                logger.warning(
                    "time_normalization_error job_id=%s error=%s",
                    state.wide_row.info_id,
                    error_type,
                )
                return state.model_copy(
                    update={
                        "error_type": error_type,
                        "route": "fallback",
                        "timing_ms": _update_timing(state, t0),
                    }
                )

            # 合并 detection + normalization → TemporalSignalRecord
            temporal = _merge_records(detection, norm_record)

            logger.info(
                "time_normalization_ok job_id=%s normalizable=%s conf=%.2f",
                state.wide_row.info_id,
                temporal.normalizable,
                temporal.confidence,
            )
            return state.model_copy(
                update={
                    "time_normalization_record": norm_record,
                    "temporal_signal_record": temporal,
                    "timing_ms": _update_timing(state, t0),
                }
            )

        except Exception:
            logger.exception(
                "time_normalization_unexpected job_id=%s",
                state.wide_row.info_id,
            )
            return state.model_copy(
                update={
                    "error_type": "unknown_time_normalization_error",
                    "route": "fallback",
                    "timing_ms": _update_timing(state, t0),
                }
            )


def _update_timing(state: GraphState, t0: float) -> dict[str, float]:
    """合并计时信息。"""
    elapsed = (time.perf_counter() - t0) * 1000
    existing = dict(state.timing_ms) if state.timing_ms else {}
    existing["time_normalization"] = round(elapsed, 2)
    return existing


def _build_skip_record(detection: Any | None) -> TemporalSignalRecord:
    """当 has_temporal_signal=False 时，从 SignalDetectionRecord 构造默认 TemporalSignalRecord。"""
    if detection is None:
        return TemporalSignalRecord()

    return TemporalSignalRecord(
        has_temporal_signal=detection.has_temporal_signal,
        temporal_status=detection.temporal_status,
        signal_type=detection.signal_type,
        evidence_summary=detection.evidence_summary,
        confidence=detection.confidence,
        cannot_determine_reason=detection.cannot_determine_reason,
        # 归一化字段保持默认值
        normalizable=False,
        work_start_at=None,
        recruitment_valid_until=None,
        duration_hours=None,
    )


def _merge_records(detection: Any, normalization: Any) -> TemporalSignalRecord:
    """合并 SignalDetectionRecord + TimeNormalizationRecord → TemporalSignalRecord。"""
    # 使用两者中较高的 confidence
    confidence = max(detection.confidence, normalization.confidence)

    return TemporalSignalRecord(
        # detection 字段
        has_temporal_signal=detection.has_temporal_signal,
        temporal_status=detection.temporal_status,
        signal_type=detection.signal_type,
        evidence_summary=detection.evidence_summary,
        cannot_determine_reason=detection.cannot_determine_reason,
        # normalization 字段
        normalizable=normalization.normalizable,
        anchor_type=normalization.anchor_type,
        work_start_at=normalization.work_start_at,
        recruitment_valid_until=normalization.recruitment_valid_until,
        duration_hours=normalization.duration_hours,
        interpretation=normalization.interpretation,
        # 合并 confidence
        confidence=confidence,
    )
