"""risk_assess 响应解析：normalize_llm_json → RiskRecord 校验。"""

from __future__ import annotations

import logging

from job_freshness.llm.result_handler import normalize_llm_json
from job_freshness.schemas import RiskRecord

logger = logging.getLogger(__name__)


def parse_risk_assess(raw_text: str) -> tuple[RiskRecord | None, str | None]:
    """解析 LLM 返回的原始文本为 RiskRecord。

    Returns:
        (record, error_type) — 成功时 error_type 为 None，失败时 record 为 None。
    """
    result = normalize_llm_json(
        raw_text,
        schema_name="RiskRecord",
        schema_model=RiskRecord,
    )
    if not result.ok:
        logger.warning(
            "risk_assess_parse_failed error_type=%s",
            result.error_type,
        )
        return None, result.error_type

    return RiskRecord.model_validate(result.data), None
