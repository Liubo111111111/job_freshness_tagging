"""FinalDecisionService — 综合 temporal_signal_record + risk_record，产出最终决策。"""

from __future__ import annotations

from datetime import datetime
import logging
import time
from typing import Any

from job_freshness.graph_state import GraphState
from job_freshness.schemas import FreshnessDecisionRecord
from job_freshness.nodes.final_decision.parser import parse_final_decision
from job_freshness.nodes.final_decision.prompt_builder import (
    build_final_decision_prompt,
)

logger = logging.getLogger(__name__)


class FinalDecisionService:
    """综合 temporal_signal_record + risk_record，产出 decision_record + route。

    路由规则：
    - low_confidence=False AND error_type=None → route="formal"
    - low_confidence=True OR error_type is not None → route="fallback"
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
        """构建 prompt → 调用 LLM → 解析 → 设置 low_confidence 和 route → 更新 GraphState。"""
        t0 = time.perf_counter()
        try:
            prompt = build_final_decision_prompt(state, self._prompt_version)
            raw_text = self._client.complete(prompt, {})
            record, error_type = parse_final_decision(raw_text)

            if error_type is not None:
                logger.warning(
                    "final_decision_error job_id=%s error=%s",
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

            record = _apply_complaint_expiry_cap(state, record)

            # 确定路由：low_confidence=False AND error_type=None → formal
            route = "formal" if not record.low_confidence else "fallback"

            logger.info(
                "final_decision_ok job_id=%s validity_type=%s low_conf=%s route=%s",
                state.wide_row.info_id,
                record.validity_type,
                record.low_confidence,
                route,
            )
            return state.model_copy(
                update={
                    "decision_record": record,
                    "route": route,
                    "timing_ms": _update_timing(state, t0),
                }
            )

        except Exception:
            logger.exception(
                "final_decision_unexpected job_id=%s",
                state.wide_row.info_id,
            )
            return state.model_copy(
                update={
                    "error_type": "unknown_final_decision_error",
                    "route": "fallback",
                    "timing_ms": _update_timing(state, t0),
                }
            )


def _update_timing(state: GraphState, t0: float) -> dict[str, float]:
    """合并计时信息。"""
    elapsed = (time.perf_counter() - t0) * 1000
    existing = dict(state.timing_ms) if state.timing_ms else {}
    existing["final_decision"] = round(elapsed, 2)
    return existing


def _apply_complaint_expiry_cap(
    state: GraphState,
    record: FreshnessDecisionRecord,
) -> FreshnessDecisionRecord:
    """若投诉侧存在已招满时间且早于预测截止时间，在 reason 中指出矛盾，但不修改 estimated_expiry。"""
    complaint_expiry = getattr(state.risk_record, "estimated_filled_at", None)
    if not complaint_expiry:
        return record

    if record.estimated_expiry is None:
        return record

    complaint_dt = _parse_datetime(complaint_expiry)
    expiry_dt = _parse_datetime(record.estimated_expiry)
    if complaint_dt is None or expiry_dt is None:
        return record

    if complaint_dt >= expiry_dt:
        return record

    # 投诉时间早于预测截止时间，存在矛盾，仅在 reason 中说明
    hint = f"注意：投诉已招满时间({complaint_expiry})早于预测截止时间，存在矛盾"
    next_reason = (record.reason or "").strip()
    if hint not in next_reason:
        next_reason = f"{next_reason}；{hint}" if next_reason else hint

    return record.model_copy(
        update={"reason": next_reason}
    )


def _parse_datetime(value: str) -> datetime | None:
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None
