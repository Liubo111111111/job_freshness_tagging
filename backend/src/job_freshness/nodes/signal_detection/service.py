"""SignalDetectionService — 从召回的 snippet 判断是否存在时效信号。"""

from __future__ import annotations

import logging
import time
from typing import Any

from job_freshness.graph_state import GraphState
from job_freshness.nodes.signal_detection.parser import (
    parse_signal_detection,
)
from job_freshness.nodes.signal_detection.prompt_builder import (
    build_signal_detection_prompt,
)

logger = logging.getLogger(__name__)


class SignalDetectionService:
    """从 state.snippet_recall_record 的时效 snippet 判断是否存在时效信号。

    仅看召回的 snippet（非 complaint 桶），不看全量文本。
    更新 state.signal_detection_record。
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
            prompt = build_signal_detection_prompt(state, self._prompt_version)
            raw_text = self._client.complete(prompt, {})
            record, error_type = parse_signal_detection(raw_text)

            if error_type is not None:
                logger.warning(
                    "signal_detection_error job_id=%s error=%s",
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
                "signal_detection_ok job_id=%s has_signal=%s type=%s conf=%.2f",
                state.wide_row.info_id,
                record.has_temporal_signal,
                record.signal_type,
                record.confidence,
            )
            return state.model_copy(
                update={
                    "signal_detection_record": record,
                    "timing_ms": _update_timing(state, t0),
                }
            )

        except Exception:
            logger.exception(
                "signal_detection_unexpected job_id=%s",
                state.wide_row.info_id,
            )
            return state.model_copy(
                update={
                    "error_type": "unknown_signal_detection_error",
                    "route": "fallback",
                    "timing_ms": _update_timing(state, t0),
                }
            )


def _update_timing(state: GraphState, t0: float) -> dict[str, float]:
    """合并计时信息。"""
    elapsed = (time.perf_counter() - t0) * 1000
    existing = dict(state.timing_ms) if state.timing_ms else {}
    existing["signal_detection"] = round(elapsed, 2)
    return existing
