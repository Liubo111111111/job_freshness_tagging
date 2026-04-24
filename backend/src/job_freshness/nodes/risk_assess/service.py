"""RiskAssessService — 纯规则节点：从投诉文本判断是否已招满及预估招满时间。

不调用 LLM，毫秒级完成。
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

from job_freshness.graph_state import GraphState
from job_freshness.schemas import RiskRecord

logger = logging.getLogger(__name__)

# 已招满关键词
_FILLED_KEYWORDS = [
    "已招满", "招满了", "人已找到", "人找到了", "已经找到",
    "不招了", "不招", "停招", "招到了", "已招到",
    "已经招到", "名额已满", "满了", "不需要了",
]

_SUSPECTED_FILLED_KEYWORDS = [
    "干完活", "干完了", "这波结束", "这批结束", "阶段结束",
    "先这样", "暂时不用", "暂时先不用", "今天先不用",
]

_ACTIVE_HIRING_KEYWORDS = [
    "还差", "差一个", "差几个人", "来一个", "来两个", "来几个人",
    "用几个", "还用", "继续用", "后面的还有", "明天还用",
    "现在还差", "正在招", "用人", "缺人",
]

# 联系不上关键词
_UNREACHABLE_KEYWORDS = [
    "电话打不通", "联系不上", "没人接", "空号", "停机",
    "打不通", "无法接通", "关机", "不接电话",
]

# 投诉标记行正则：【投诉N 时间】
_TEXT_HEADER_RE = re.compile(
    r"【(?:投诉|通话|会话)\d+\s+(\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{1,2}:\d{1,2})】"
)


def has_filled_complaint_signal(complaint_text: str) -> bool:
    """兼容旧逻辑：仅判断文本中是否存在明确“已招满”信号。"""
    return any(keyword in complaint_text for keyword in _FILLED_KEYWORDS)


class RiskAssessService:
    """纯规则：从投诉文本判断是否已招满，提取投诉时间作为预估招满时间。

    不需要 LLM client，不需要 prompt。
    """

    def __init__(self, **kwargs: Any) -> None:
        # 接受任意参数以兼容旧接口（client, model_version 等），但不使用
        pass

    def run(self, state: GraphState) -> GraphState:
        """纯规则分析 ASR/IM/投诉文本，更新 state.risk_record。"""
        t0 = time.perf_counter()
        complaint = state.wide_row.complaint_content or ""
        asr_text = state.wide_row.asr_result or ""
        im_text = state.wide_row.im_text or ""
        corpus = "\n".join(part for part in [complaint, asr_text, im_text] if part.strip())

        is_filled = False
        fill_status = "not_filled"
        is_unreachable = False
        complaint_summary = ""
        estimated_filled_at: str | None = None
        estimated_filled_reason = ""

        if corpus.strip():
            has_confirmed_filled = any(kw in corpus for kw in _FILLED_KEYWORDS)
            has_suspected_filled = any(kw in corpus for kw in _SUSPECTED_FILLED_KEYWORDS)
            has_active_hiring = any(kw in corpus for kw in _ACTIVE_HIRING_KEYWORDS)

            # 判断是否联系不上
            for kw in _UNREACHABLE_KEYWORDS:
                if kw in corpus:
                    is_unreachable = True
                    break

            if has_confirmed_filled and not has_active_hiring:
                fill_status = "confirmed_filled"
            elif has_confirmed_filled and has_active_hiring:
                fill_status = "suspected_filled"
            elif has_suspected_filled and not has_active_hiring:
                fill_status = "suspected_filled"
            else:
                fill_status = "not_filled"

            is_filled = fill_status == "confirmed_filled"

            # 生成摘要
            if fill_status == "confirmed_filled" and is_unreachable:
                complaint_summary = "已招满且联系不上"
            elif fill_status == "confirmed_filled":
                complaint_summary = "已招满"
            elif fill_status == "suspected_filled" and has_active_hiring:
                complaint_summary = "存在阶段性结束或招满信号，但仍有继续招人迹象"
            elif fill_status == "suspected_filled":
                complaint_summary = "阶段性结束，疑似招满"
            elif is_unreachable:
                complaint_summary = "联系不上"
            else:
                # 有投诉但不匹配已知模式
                complaint_summary = corpus[:50].replace("\n", " ")

            # 提取最早的文本时间戳作为预估招满时间
            if fill_status in {"confirmed_filled", "suspected_filled"}:
                timestamps = _TEXT_HEADER_RE.findall(corpus)
                if timestamps:
                    earliest = sorted(timestamps)[0]
                    estimated_filled_at = earliest.replace(" ", "T") + "+08:00"
                    if fill_status == "confirmed_filled":
                        estimated_filled_reason = f"文本时间 {earliest} 明确表达已招满，实际招满时间不晚于此"
                    else:
                        estimated_filled_reason = f"文本时间 {earliest} 出现阶段性结束信号，疑似招满时间不晚于此"

        record = RiskRecord(
            is_filled=is_filled,
            fill_status=fill_status,
            is_unreachable=is_unreachable,
            complaint_summary=complaint_summary,
            estimated_filled_at=estimated_filled_at,
            estimated_filled_reason=estimated_filled_reason,
        )

        logger.info(
            "risk_assess_ok job_id=%s fill_status=%s is_unreachable=%s filled_at=%s",
            state.wide_row.info_id,
            fill_status,
            is_unreachable,
            estimated_filled_at,
        )

        return state.model_copy(
            update={
                "risk_record": record,
                "timing_ms": _update_timing(state, t0),
            }
        )


def _update_timing(state: GraphState, t0: float) -> dict[str, float]:
    elapsed = (time.perf_counter() - t0) * 1000
    existing = dict(state.timing_ms) if state.timing_ms else {}
    existing["risk_assess"] = round(elapsed, 2)
    return existing
