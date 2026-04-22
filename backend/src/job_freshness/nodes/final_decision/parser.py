"""final_decision 响应解析：normalize_llm_json → FreshnessDecisionRecord 校验。"""

from __future__ import annotations

import logging

from job_freshness.llm.result_handler import normalize_llm_json
from job_freshness.schemas import FreshnessDecisionRecord

logger = logging.getLogger(__name__)


def parse_final_decision(raw_text: str) -> tuple[FreshnessDecisionRecord | None, str | None]:
    """解析 LLM 返回的原始文本为 FreshnessDecisionRecord。

    Returns:
        (record, error_type) — 成功时 error_type 为 None，失败时 record 为 None。
    """
    result = normalize_llm_json(
        raw_text,
        schema_name="FreshnessDecisionRecord",
        schema_model=FreshnessDecisionRecord,
    )
    if not result.ok:
        logger.warning(
            "final_decision_parse_failed error_type=%s",
            result.error_type,
        )
        return None, result.error_type

    return FreshnessDecisionRecord.model_validate(result.data), None
