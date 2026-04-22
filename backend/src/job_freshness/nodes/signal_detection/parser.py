"""signal_detection 响应解析：normalize_llm_json → SignalDetectionRecord 校验。"""

from __future__ import annotations

import logging

from job_freshness.llm.result_handler import normalize_llm_json
from job_freshness.schemas import SignalDetectionRecord

logger = logging.getLogger(__name__)


def parse_signal_detection(raw_text: str) -> tuple[SignalDetectionRecord | None, str | None]:
    """解析 LLM 返回的原始文本为 SignalDetectionRecord。

    Returns:
        (record, error_type) — 成功时 error_type 为 None，失败时 record 为 None。
    """
    result = normalize_llm_json(
        raw_text,
        schema_name="SignalDetectionRecord",
        schema_model=SignalDetectionRecord,
    )
    if not result.ok:
        logger.warning(
            "signal_detection_parse_failed error_type=%s",
            result.error_type,
        )
        return None, result.error_type

    return SignalDetectionRecord.model_validate(result.data), None
