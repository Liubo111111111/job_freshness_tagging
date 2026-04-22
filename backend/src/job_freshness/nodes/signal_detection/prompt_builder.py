"""signal_detection prompt 构建：加载 YAML 模板 + 填充全文 + 规则命中摘要。"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from job_freshness.graph_state import GraphState
from job_freshness.schemas import RiskContext

logger = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).resolve().parent.parent.parent / "prompts"


@lru_cache(maxsize=4)
def _load_prompt_template(version: str) -> dict[str, str]:
    """加载并缓存 prompt YAML 模板。"""
    path = _PROMPTS_DIR / f"signal_detection_{version}.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_signal_detection_payload(state: GraphState) -> dict[str, Any]:
    """构建传给 LLM 的 payload（全文 + 非 complaint 规则命中摘要 + 辅助上下文）。"""
    wide = state.wide_row
    recall = state.snippet_recall_record

    # 全文证据（由 WideRow 清洗后的文本）
    evidence_text: dict[str, str] = {
        "job_detail": wide.job_detail,
        "im_text": wide.im_text,
        "asr_text": wide.asr_result,
    }

    # 规则命中摘要（仅非 complaint 桶）
    rule_matches: list[dict[str, Any]] = []
    if recall:
        for m in recall.matches:
            if m.matched_bucket != "complaint":
                rule_matches.append({
                    "source": m.source,
                    "matched_bucket": m.matched_bucket,
                    "matched_terms": m.matched_terms,
                })

    risk_context = RiskContext(
        complaint_text=wide.complaint_content,
        im_message_count=wide.im_message_count,
        call_record_count=wide.call_record_count,
        complaint_count=wide.complaint_count,
    )

    return {
        "job_id": wide.info_id,
        "publish_time": wide.publish_time,
        "evidence_text": evidence_text,
        "rule_matches": rule_matches,
        "risk_context": risk_context.model_dump(),
    }


def build_signal_detection_prompt(state: GraphState, prompt_version: str) -> str:
    """构建完整的 signal_detection prompt 字符串（system + user）。"""
    template = _load_prompt_template(prompt_version)
    payload = build_signal_detection_payload(state)

    system_msg = template["system"]
    user_template = template["user"]
    user_msg = user_template.replace("{{payload}}", json.dumps(payload, ensure_ascii=False, indent=2))

    return f"[TASK: signal_detection]\n[SYSTEM]\n{system_msg}\n[USER]\n{user_msg}"
