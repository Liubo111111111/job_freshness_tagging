"""time_normalization 响应解析：normalize_llm_json → TimeNormalizationRecord 校验。"""

from __future__ import annotations

import logging

from job_freshness.llm.result_handler import normalize_llm_json
from job_freshness.schemas import TimeNormalizationRecord

logger = logging.getLogger(__name__)


def parse_time_normalization(raw_text: str) -> tuple[TimeNormalizationRecord | None, str | None]:
    """解析 LLM 返回的原始文本为 TimeNormalizationRecord。

    Returns:
        (record, error_type) — 成功时 error_type 为 None，失败时 record 为 None。
    """
    result = normalize_llm_json(
        raw_text,
        schema_name="TimeNormalizationRecord",
        schema_model=TimeNormalizationRecord,
    )
    if not result.ok:
        logger.warning(
            "time_normalization_parse_failed error_type=%s",
            result.error_type,
        )
        return None, result.error_type

    return TimeNormalizationRecord.model_validate(result.data), None
