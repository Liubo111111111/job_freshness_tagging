"""RiskAssessService — 从投诉文本和计数特征评估风险。"""

from __future__ import annotations

import logging
import time
from typing import Any

from job_freshness.graph_state import GraphState
from job_freshness.nodes.risk_assess.parser import parse_risk_assess
from job_freshness.nodes.risk_assess.prompt_builder import (
    build_risk_assess_prompt,
)

logger = logging.getLogger(__name__)


class RiskAssessService:
    """从 state.wide_row 的投诉文本和计数特征评估风险，更新 state.risk_record。

    注意：并行执行时不依赖 temporal_signal_record，仅使用 WideRow 中的辅助上下文。
    本节点产出风险提示，不可提取或覆写时间戳。
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
        """构建 prompt → 调用 LLM → 解析 → 更新 GraphState。"""
        t0 = time.perf_counter()
        try:
            prompt = build_risk_assess_prompt(state, self._prompt_version)
            raw_text = self._client.complete(prompt, {})
            record, error_type = parse_risk_assess(raw_text)

            if error_type is not None:
                logger.warning(
                    "risk_assess_error job_id=%s error=%s",
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

            logger.info(
                "risk_assess_ok job_id=%s stale_risk=%s complaint=%s score=%.2f conf=%.2f",
                state.wide_row.info_id,
                record.stale_risk_hint,
                record.complaint_risk_hint.has_complaint_signal,
                record.risk_score,
                record.confidence,
            )
            return state.model_copy(
                update={
                    "risk_record": record,
                    "timing_ms": _update_timing(state, t0),
                }
            )

        except Exception:
            logger.exception(
                "risk_assess_unexpected job_id=%s",
                state.wide_row.info_id,
            )
            return state.model_copy(
                update={
                    "error_type": "unknown_risk_assess_error",
                    "route": "fallback",
                    "timing_ms": _update_timing(state, t0),
                }
            )


def _update_timing(state: GraphState, t0: float) -> dict[str, float]:
    """合并计时信息。"""
    elapsed = (time.perf_counter() - t0) * 1000
    existing = dict(state.timing_ms) if state.timing_ms else {}
    existing["risk_assess"] = round(elapsed, 2)
    return existing
