"""snippet_recall 节点单元测试。"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from job_freshness.graph_state import GraphState
from job_freshness.nodes.snippet_recall.service import SnippetRecallService
from job_freshness.schemas import WideRow

# 使用项目根目录的词表配置
_LEXICON_PATH = Path(__file__).resolve().parents[3] / "config" / "recall_lexicon_v1.json"


def _make_state(
    job_detail: str = "",
    im_text: str = "",
    asr_result: str = "",
) -> GraphState:
    return GraphState(
        run_id="test-run",
        entity_key="test-info",
        feature_schema_version="v1",
        graph_version="v1",
        prompt_version_detection="v1",
        prompt_version_normalization="v1",
        prompt_version_risk="v1",
        prompt_version_final="v1",
        model_version_detection="v1",
        model_version_normalization="v1",
        model_version_risk="v1",
        model_version_final="v1",
        wide_row=WideRow(
            user_id="u1",
            info_id="i1",
            job_detail=job_detail,
            im_text=im_text,
            asr_result=asr_result,
        ),
    )


@pytest.fixture(scope="module")
def service() -> SnippetRecallService:
    return SnippetRecallService(_LEXICON_PATH)


class TestJobDetailExtraction:
    """job_detail: 按句号/换行分句，取命中所在句。"""

    def test_single_sentence_with_term(self, service: SnippetRecallService):
        state = _make_state(job_detail="明天下午开工，要5个小工")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.has_recall is True
        assert rec.temporal_snippet_count > 0
        # 应该命中 "明天"（absolute_time）和 "开工"（recruitment_action）
        buckets = {s.matched_bucket for s in rec.snippets if s.source == "job_detail"}
        assert "absolute_time" in buckets or "recruitment_action" in buckets

    def test_multi_sentence_only_hit_sentence(self, service: SnippetRecallService):
        state = _make_state(job_detail="这是一个普通描述。明天开工需要人。另一段无关内容")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        # 命中句应包含 "明天"
        hit_snippets = [s for s in rec.snippets if s.source == "job_detail"]
        assert len(hit_snippets) > 0
        assert any("明天" in s.text for s in hit_snippets)

    def test_date_regex_match(self, service: SnippetRecallService):
        state = _make_state(job_detail="4月5号入场，做两天")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.has_recall is True
        terms = [t for s in rec.snippets for t in s.matched_terms]
        assert any("4月5号" in t for t in terms)


class TestImTextExtraction:
    """im_text: 按换行分消息，取命中消息 + 前后各 1 条消息窗口。"""

    def test_neighbor_window(self, service: SnippetRecallService):
        state = _make_state(im_text="你好\n明天来上班\n好的收到\n再见")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        hit_snippets = [s for s in rec.snippets if s.source == "im_text"]
        assert len(hit_snippets) > 0
        # 应包含邻居消息
        snippet_text = hit_snippets[0].text
        assert "你好" in snippet_text  # 前 1 条
        assert "明天" in snippet_text  # 命中消息
        assert "好的收到" in snippet_text  # 后 1 条


class TestAsrTextExtraction:
    """asr_text: 按句号/换行分句，取命中句 + 前后各 1 句邻句。"""

    def test_neighbor_window(self, service: SnippetRecallService):
        state = _make_state(asr_result="前面的话。后天到岗。后面的话。最后一句")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        hit_snippets = [s for s in rec.snippets if s.source == "asr_text"]
        assert len(hit_snippets) > 0
        snippet_text = hit_snippets[0].text
        assert "前面的话" in snippet_text
        assert "后天" in snippet_text
        assert "后面的话" in snippet_text


class TestComplaintSeparation:
    """complaint_terms 命中的 snippet 标记为 complaint 桶，单独计数。"""

    def test_complaint_bucket_tagging(self, service: SnippetRecallService):
        state = _make_state(job_detail="已招满，不再招人")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        complaint_snippets = [s for s in rec.snippets if s.matched_bucket == "complaint"]
        assert len(complaint_snippets) > 0
        assert rec.complaint_snippet_count > 0

    def test_mixed_temporal_and_complaint(self, service: SnippetRecallService):
        state = _make_state(job_detail="明天开工。已招满不招了")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.temporal_snippet_count > 0
        assert rec.complaint_snippet_count > 0


class TestDeduplication:
    """同一来源内完全相同的 snippet 去重。"""

    def test_exact_duplicate_removed(self, service: SnippetRecallService):
        # 同一句中多个词属于同一桶，应只产出一个 snippet
        state = _make_state(job_detail="今天明天都可以来")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        # 同一桶同一文本只出现一次
        seen = set()
        for s in rec.snippets:
            key = (s.source, s.matched_bucket, s.text)
            assert key not in seen, f"重复 snippet: {key}"
            seen.add(key)


class TestEmptyInput:
    """空文本输入应返回 has_recall=False。"""

    def test_all_empty(self, service: SnippetRecallService):
        state = _make_state()
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.has_recall is False
        assert rec.temporal_snippet_count == 0
        assert rec.complaint_snippet_count == 0
        assert len(rec.snippets) == 0

    def test_no_match_text(self, service: SnippetRecallService):
        state = _make_state(job_detail="这是一段完全无关的文字描述")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.has_recall is False
