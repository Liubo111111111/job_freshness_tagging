"""TextCleaningService — 纯规则节点：清洗 WideRow 中的文本字段，不调用 LLM。"""

from __future__ import annotations

import logging

from job_freshness.graph_state import GraphState
from job_freshness.text_cleaning import clean_wide_row_texts

logger = logging.getLogger(__name__)


class TextCleaningService:
    """纯规则：清洗 WideRow 中的文本字段（ASR 展平、IM 清洗、降噪），返回更新后的 GraphState。"""

    def run(self, state: GraphState) -> GraphState:
        """清洗 WideRow 中的文本字段，返回更新后的 GraphState。"""
        cleaned_row = clean_wide_row_texts(state.wide_row)
        return state.model_copy(update={
            "raw_wide_row": state.wide_row,  # 保存原始数据
            "wide_row": cleaned_row,          # 替换为清洗后数据
        })
