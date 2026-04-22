"""time_normalization prompt 构建：加载 YAML 模板 + 填充检测结果和原始文本。"""

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
    path = _PROMPTS_DIR / f"time_normalization_{version}.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_time_normalization_payload(state: GraphState) -> dict[str, Any]:
    """构建传给 LLM 的 payload（signal_detection 结果 + 原始 evidence_text）。"""
    wide = state.wide_row
    detection = state.signal_detection_record

    # signal_detection 结果（仅传关键字段）
    signal_detection: dict[str, Any] = {}
    if detection:
        signal_detection = {
            "temporal_status": detection.temporal_status,
            "signal_type": detection.signal_type,
            "evidence_summary": detection.evidence_summary,
        }

    # 原始文本证据
    evidence_text = {
        "job_detail": wide.job_detail,
        "im_text": wide.im_text,
        "asr_text": wide.asr_result,
    }

    return {
        "job_id": wide.info_id,
        "publish_time": wide.publish_time,
        "signal_detection": signal_detection,
        "evidence_text": evidence_text,
    }


def build_time_normalization_prompt(state: GraphState, prompt_version: str) -> str:
    """构建完整的 time_normalization prompt 字符串（system + user）。"""
    template = _load_prompt_template(prompt_version)
    payload = build_time_normalization_payload(state)

    system_msg = template["system"]
    user_template = template["user"]
    user_msg = user_template.replace(
        "{{payload}}", json.dumps(payload, ensure_ascii=False, indent=2)
    )

    return f"[TASK: time_normalization]\n[SYSTEM]\n{system_msg}\n[USER]\n{user_msg}"
