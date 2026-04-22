"""SnippetRecallService — 纯规则命中检测：词表 + 正则检测匹配项，不提取文本片段。"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from job_freshness.graph_state import GraphState
from job_freshness.nodes.snippet_recall.lexicon import Lexicon
from job_freshness.nodes.snippet_recall.patterns import (
    find_all_matches,
)
from job_freshness.schemas import RuleMatch, SnippetRecallRecord

logger = logging.getLogger(__name__)

# 元数据标记行正则：【通话N ...】、【会话N ...】、【投诉N ...】
_HEADER_LINE_RE = re.compile(r"^【(通话|会话|投诉)\d+\s+[^】]*】$")


def _strip_header_lines(text: str) -> str:
    """移除元数据标记行，只保留实际内容行用于规则匹配。"""
    lines = text.splitlines()
    content_lines = [line for line in lines if not _HEADER_LINE_RE.match(line.strip())]
    return "\n".join(content_lines)


class SnippetRecallService:
    """纯规则命中检测：从 state.wide_row 的文本字段中用词表+正则检测匹配项。"""

    def __init__(self, lexicon_path: str | Path) -> None:
        self._lexicon = Lexicon(lexicon_path)

    def run(self, state: GraphState) -> GraphState:
        """纯规则命中检测：检测哪些词/模式在哪些来源中命中，
        更新 state.snippet_recall_record，不调用 LLM。"""
        wide = state.wide_row
        all_matches: list[RuleMatch] = []
        matched_sources: set[str] = set()

        for source_name, text in [
            ("job_detail", wide.job_detail),
            ("im_text", wide.im_text),
            ("asr_text", wide.asr_result),
        ]:
            if not text.strip():
                continue
            # 移除元数据标记行（【通话N 时间】等），只匹配实际内容
            content_text = _strip_header_lines(text)
            if not content_text.strip():
                continue
            hits = find_all_matches(
                content_text, self._lexicon.term_patterns, self._lexicon.regex_patterns
            )
            if not hits:
                continue
            matched_sources.add(source_name)
            # 按 bucket 分组并去重 terms
            bucket_terms: dict[str, set[str]] = {}
            for h in hits:
                canonical = _canonicalize_bucket(h.bucket)
                bucket_terms.setdefault(canonical, set()).add(h.term)
            for bucket, terms in bucket_terms.items():
                all_matches.append(
                    RuleMatch(
                        source=source_name,  # type: ignore[arg-type]
                        matched_terms=sorted(terms),
                        matched_bucket=bucket,
                    )
                )

        temporal_count = sum(
            1 for m in all_matches if m.matched_bucket != "complaint"
        )
        complaint_count = sum(
            1 for m in all_matches if m.matched_bucket == "complaint"
        )

        record = SnippetRecallRecord(
            has_recall=temporal_count > 0,  # 仅非 complaint 命中才算有召回
            matches=all_matches,
            temporal_match_count=temporal_count,
            complaint_match_count=complaint_count,
            matched_sources=sorted(matched_sources),
        )

        return state.model_copy(update={"snippet_recall_record": record})


# 词表桶名 → schema 中的 matched_bucket 值映射
_BUCKET_CANONICAL_MAP: dict[str, str] = {
    "absolute_time_terms": "absolute_time",
    "recruitment_action_terms": "recruitment_action",
    "duration_terms": "duration",
    "deadline_terms": "deadline",
    "holiday_terms": "holiday",
    "complaint_terms": "complaint",
    # 正则组映射的虚拟桶名已经是规范名
    "absolute_time": "absolute_time",
    "duration": "duration",
}


def _canonicalize_bucket(bucket: str) -> str:
    """将词表桶名或正则组映射的桶名规范化为 schema 中的 matched_bucket 值。"""
    return _BUCKET_CANONICAL_MAP.get(bucket, bucket)
