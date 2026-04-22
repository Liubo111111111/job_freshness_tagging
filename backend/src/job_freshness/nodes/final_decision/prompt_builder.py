"""final_decision prompt 构建：加载 YAML 模板 + 填充 temporal_signal_record 和 risk_record。"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from job_freshness.graph_state import GraphState

logger = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).resolve().parent.parent.parent / "prompts"


@lru_cache(maxsize=4)
def _load_prompt_template(version: str) -> dict[str, str]:
    """加载并缓存 prompt YAML 模板。"""
    path = _PROMPTS_DIR / f"final_decision_{version}.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_final_decision_payload(state: GraphState) -> dict[str, Any]:
    """构建传给 LLM 的 payload（temporal_signal_record + risk_record）。"""
    wide = state.wide_row

    temporal_signal_data: dict[str, Any] = {}
    if state.temporal_signal_record is not None:
        temporal_signal_data = state.temporal_signal_record.model_dump()

    risk_data: dict[str, Any] = {}
    if state.risk_record is not None:
        risk_data = state.risk_record.model_dump()

    return {
        "job_id": wide.info_id,
        "publish_time": wide.publish_time,
        "temporal_signal_record": temporal_signal_data,
        "risk_record": risk_data,
    }


def build_final_decision_prompt(state: GraphState, prompt_version: str) -> str:
    """构建完整的 final_decision prompt 字符串（system + user）。"""
    template = _load_prompt_template(prompt_version)
    payload = build_final_decision_payload(state)

    system_msg = template["system"]
    user_template = template["user"]
    user_msg = user_template.replace("{{payload}}", json.dumps(payload, ensure_ascii=False, indent=2))

    return f"[TASK: final_decision]\n[SYSTEM]\n{system_msg}\n[USER]\n{user_msg}"
