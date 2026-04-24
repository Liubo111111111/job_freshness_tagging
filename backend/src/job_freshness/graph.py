"""LangGraph 图定义 — 职位新鲜度识别流水线。

六节点流水线：
  text_cleaning（纯规则）→ snippet_recall（纯规则）→ fan-out signal_detection + risk_assess（并行）
  → conditional time_normalization → final_decision → formal_output / fallback_output
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from langgraph.graph import END, StateGraph

from job_freshness.graph_state import GraphState
from job_freshness.llm.client import HttpLLMClient
from job_freshness.nodes.final_decision import FinalDecisionService
from job_freshness.nodes.risk_assess import RiskAssessService
from job_freshness.nodes.signal_detection import SignalDetectionService
from job_freshness.nodes.snippet_recall import SnippetRecallService
from job_freshness.nodes.text_cleaning import TextCleaningService
from job_freshness.nodes.time_normalization import TimeNormalizationService
from job_freshness.settings import load_llm_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _diff_state(original: GraphState, updated: GraphState) -> dict[str, Any]:
    """比较两个 GraphState，返回变更的字段（dict 形式）。

    LangGraph 的 dict state 使用 merge 语义，只需返回变更的字段即可。
    """
    original_dump = original.model_dump()
    updated_dump = updated.model_dump()
    diff: dict[str, Any] = {}
    for key, new_val in updated_dump.items():
        if new_val != original_dump.get(key):
            diff[key] = new_val
    return diff


# ---------------------------------------------------------------------------
# 默认版本号（可通过 GraphState 覆盖）
# ---------------------------------------------------------------------------

_DEFAULT_PROMPT_VERSION_DETECTION = "v1"
_DEFAULT_PROMPT_VERSION_NORMALIZATION = "v1"
_DEFAULT_PROMPT_VERSION_RISK = "v1"
_DEFAULT_PROMPT_VERSION_FINAL = "v1"
_DEFAULT_MODEL_VERSION = "qwen3-max"

# 词表配置路径
_LEXICON_PATH = Path(__file__).resolve().parents[3] / "config" / "recall_lexicon_v1.json"


# ---------------------------------------------------------------------------
# 条件路由函数
# ---------------------------------------------------------------------------


def _route_after_detection(state: dict[str, Any]) -> str:
    """条件路由：has_temporal_signal=True → time_normalization，否则跳过直接到 final_decision。"""
    detection = state.get("signal_detection_record") or {}
    if isinstance(detection, dict) and detection.get("has_temporal_signal"):
        return "normalize"
    return "skip"


def _route_after_final(state: dict[str, Any]) -> str:
    """条件路由：low_confidence=False AND error_type=None → formal_output，否则 fallback_output。"""
    decision = state.get("decision_record") or {}
    error_type = state.get("error_type")

    if error_type is not None:
        return "fallback"

    if isinstance(decision, dict):
        low_confidence = decision.get("low_confidence", True)
    else:
        low_confidence = True

    if not low_confidence:
        return "formal"
    return "fallback"


# ---------------------------------------------------------------------------
# 节点包装函数（dict ↔ GraphState 转换）
# ---------------------------------------------------------------------------


def _text_cleaning_node(state: dict[str, Any]) -> dict[str, Any]:
    """text_cleaning 节点：纯规则，清洗 WideRow 中的文本字段。"""
    graph_state = GraphState.model_validate(state)
    service = TextCleaningService()
    updated = service.run(graph_state)
    return _diff_state(graph_state, updated)


def _snippet_recall_node(state: dict[str, Any]) -> dict[str, Any]:
    """snippet_recall 节点：纯规则，词表+正则截取 snippet。"""
    graph_state = GraphState.model_validate(state)
    service = SnippetRecallService(lexicon_path=_LEXICON_PATH)
    updated = service.run(graph_state)
    # 返回变更的字段
    return _diff_state(graph_state, updated)


def _signal_detection_node(state: dict[str, Any]) -> dict[str, Any]:
    """signal_detection 节点：从召回的 snippet 判断是否存在时效信号。"""
    graph_state = GraphState.model_validate(state)
    settings = load_llm_settings()
    client = HttpLLMClient(settings)
    service = SignalDetectionService(
        client=client,
        model_version=graph_state.model_version_detection,
        prompt_version=graph_state.prompt_version_detection,
    )
    updated = service.run(graph_state)
    return _diff_state(graph_state, updated)


def _time_normalization_node(state: dict[str, Any]) -> dict[str, Any]:
    """time_normalization 节点：条件执行，仅 has_temporal_signal=True 时调用 LLM 归一化。"""
    graph_state = GraphState.model_validate(state)
    settings = load_llm_settings()
    client = HttpLLMClient(settings)
    service = TimeNormalizationService(
        client=client,
        model_version=graph_state.model_version_normalization,
        prompt_version=graph_state.prompt_version_normalization,
    )
    updated = service.run(graph_state)
    return _diff_state(graph_state, updated)


def _risk_assess_node(state: dict[str, Any]) -> dict[str, Any]:
    """risk_assess 节点：纯规则，从投诉文本判断是否已招满。"""
    graph_state = GraphState.model_validate(state)
    service = RiskAssessService()
    updated = service.run(graph_state)
    return _diff_state(graph_state, updated)


def _final_decision_node(state: dict[str, Any]) -> dict[str, Any]:
    """final_decision 节点：综合 temporal_signal_record + risk_record 产出最终决策。"""
    graph_state = GraphState.model_validate(state)
    settings = load_llm_settings()
    client = HttpLLMClient(settings)
    service = FinalDecisionService(
        client=client,
        model_version=graph_state.model_version_final,
        prompt_version=graph_state.prompt_version_final,
    )
    updated = service.run(graph_state)
    return _diff_state(graph_state, updated)


def _formal_output_node(state: dict[str, Any]) -> dict[str, Any]:
    """formal_output 节点：高置信结果写入 JSONL + SQLite。"""
    graph_state = GraphState.model_validate(state)
    decision = graph_state.decision_record

    record: dict[str, Any] = {
        "info_id": graph_state.entity_key,
        "validity_type": decision.validity_type if decision else "no_validity",
        "estimated_expiry": decision.estimated_expiry if decision else None,
        "reason": decision.reason if decision else "",
        "audit": {
            "run_id": graph_state.run_id,
            "entity_key": graph_state.entity_key,
            "graph_version": graph_state.graph_version,
            "feature_schema_version": graph_state.feature_schema_version,
            "prompt_version_detection": graph_state.prompt_version_detection,
            "prompt_version_normalization": graph_state.prompt_version_normalization,
            "prompt_version_risk": graph_state.prompt_version_risk,
            "prompt_version_final": graph_state.prompt_version_final,
            "model_version_detection": graph_state.model_version_detection,
            "model_version_normalization": graph_state.model_version_normalization,
            "model_version_risk": graph_state.model_version_risk,
            "model_version_final": graph_state.model_version_final,
        },
    }

    logger.info(
        "formal_output job_id=%s validity_type=%s",
        graph_state.entity_key,
        record["validity_type"],
    )
    # 实际写入由外部 batch runner 负责，此处仅标记 route
    return {"route": "formal"}


def _fallback_output_node(state: dict[str, Any]) -> dict[str, Any]:
    """fallback_output 节点：低置信/错误结果写入 JSONL + SQLite。"""
    graph_state = GraphState.model_validate(state)

    record: dict[str, Any] = {
        "info_id": graph_state.entity_key,
        "error_type": graph_state.error_type,
        "decision_record": (
            graph_state.decision_record.model_dump()
            if graph_state.decision_record
            else None
        ),
        "audit": {
            "run_id": graph_state.run_id,
            "entity_key": graph_state.entity_key,
            "error_type": graph_state.error_type,
            "graph_version": graph_state.graph_version,
            "feature_schema_version": graph_state.feature_schema_version,
        },
    }

    logger.info(
        "fallback_output job_id=%s error_type=%s",
        graph_state.entity_key,
        graph_state.error_type,
    )
    # 实际写入由外部 batch runner 负责，此处仅标记 route
    return {"route": "fallback"}


# ---------------------------------------------------------------------------
# 图构建
# ---------------------------------------------------------------------------


def build_graph():
    """构建并编译 LangGraph 流水线图。

    节点拓扑：
      __start__ → text_cleaning → snippet_recall → [signal_detection, risk_assess]（并行）
      signal_detection → (条件) time_normalization / final_decision
      time_normalization → final_decision
      risk_assess → final_decision
      final_decision → (条件) formal_output / fallback_output
      formal_output → END
      fallback_output → END
    """
    workflow = StateGraph(dict)

    # 注册节点
    workflow.add_node("text_cleaning", _text_cleaning_node)
    workflow.add_node("snippet_recall", _snippet_recall_node)
    workflow.add_node("signal_detection", _signal_detection_node)
    workflow.add_node("time_normalization", _time_normalization_node)
    workflow.add_node("risk_assess", _risk_assess_node)
    workflow.add_node("final_decision", _final_decision_node)
    workflow.add_node("formal_output", _formal_output_node)
    workflow.add_node("fallback_output", _fallback_output_node)

    # text_cleaning 先执行（纯规则，无 LLM）
    workflow.add_edge("__start__", "text_cleaning")

    # text_cleaning 完成后进入 snippet_recall（纯规则，无 LLM）
    workflow.add_edge("text_cleaning", "snippet_recall")

    # snippet_recall 完成后扇出：signal_detection 和 risk_assess 并行
    workflow.add_edge("snippet_recall", "signal_detection")
    workflow.add_edge("snippet_recall", "risk_assess")

    # signal_detection 后条件路由
    workflow.add_conditional_edges(
        "signal_detection",
        _route_after_detection,
        {"normalize": "time_normalization", "skip": "final_decision"},
    )

    # time_normalization 完成后汇聚到 final_decision
    workflow.add_edge("time_normalization", "final_decision")
    # risk_assess 完成后汇聚到 final_decision
    workflow.add_edge("risk_assess", "final_decision")

    # final_decision 后条件路由到输出节点
    workflow.add_conditional_edges(
        "final_decision",
        _route_after_final,
        {"formal": "formal_output", "fallback": "fallback_output"},
    )

    # 输出节点到终止
    workflow.add_edge("formal_output", END)
    workflow.add_edge("fallback_output", END)

    return workflow.compile()
