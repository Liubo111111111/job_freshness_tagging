"""risk_assess prompt 构建：加载 YAML 模板 + 填充 risk_context。"""

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
    path = _PROMPTS_DIR / f"risk_assess_{version}.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_risk_assess_payload(state: GraphState) -> dict[str, Any]:
    """构建传给 LLM 的 payload（仅 risk_context：投诉文本 + 计数特征）。"""
    wide = state.wide_row

    risk_context = RiskContext(
        complaint_text=wide.complaint_content,
        im_message_count=wide.im_message_count,
        call_record_count=wide.call_record_count,
        complaint_count=wide.complaint_count,
    )

    return {
        "job_id": wide.info_id,
        "publish_time": wide.publish_time,
        "risk_context": risk_context.model_dump(),
    }


def build_risk_assess_prompt(state: GraphState, prompt_version: str) -> str:
    """构建完整的 risk_assess prompt 字符串（system + user）。"""
    template = _load_prompt_template(prompt_version)
    payload = build_risk_assess_payload(state)

    system_msg = template["system"]
    user_template = template["user"]
    user_msg = user_template.replace("{{payload}}", json.dumps(payload, ensure_ascii=False, indent=2))

    return f"[TASK: risk_assess]\n[SYSTEM]\n{system_msg}\n[USER]\n{user_msg}"
