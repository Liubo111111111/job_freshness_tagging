"""snippet_recall 节点单元测试（规则命中检测）。"""

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


class TestJobDetailMatching:
    """job_detail: 检测词表和正则命中。"""

    def test_single_sentence_with_term(self, service: SnippetRecallService):
        state = _make_state(job_detail="明天下午开工，要5个小工")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.has_recall is True
        assert rec.temporal_match_count > 0
        # 应该命中 "明天"（absolute_time）和 "开工"（recruitment_action）
        buckets = {m.matched_bucket for m in rec.matches if m.source == "job_detail"}
        assert "absolute_time" in buckets or "recruitment_action" in buckets
        assert "job_detail" in rec.matched_sources

    def test_multi_sentence_match(self, service: SnippetRecallService):
        state = _make_state(job_detail="这是一个普通描述。明天开工需要人。另一段无关内容")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        # 应命中 "明天" 相关词
        hit_matches = [m for m in rec.matches if m.source == "job_detail"]
        assert len(hit_matches) > 0
        all_terms = [t for m in hit_matches for t in m.matched_terms]
        assert any("明天" in t for t in all_terms)

    def test_date_regex_match(self, service: SnippetRecallService):
        state = _make_state(job_detail="4月5号入场，做两天")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.has_recall is True
        terms = [t for m in rec.matches for t in m.matched_terms]
        assert any("4月5号" in t for t in terms)


class TestImTextMatching:
    """im_text: 检测 IM 消息中的命中。"""

    def test_im_match(self, service: SnippetRecallService):
        state = _make_state(im_text="你好\n明天来上班\n好的收到\n再见")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        hit_matches = [m for m in rec.matches if m.source == "im_text"]
        assert len(hit_matches) > 0
        assert "im_text" in rec.matched_sources


class TestAsrTextMatching:
    """asr_text: 检测 ASR 转写中的命中。"""

    def test_asr_match(self, service: SnippetRecallService):
        state = _make_state(asr_result="前面的话。后天到岗。后面的话。最后一句")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        hit_matches = [m for m in rec.matches if m.source == "asr_text"]
        assert len(hit_matches) > 0
        assert "asr_text" in rec.matched_sources


class TestComplaintSeparation:
    """complaint_terms 命中标记为 complaint 桶，单独计数。"""

    def test_complaint_bucket_tagging(self, service: SnippetRecallService):
        state = _make_state(job_detail="已招满，不再招人")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        complaint_matches = [m for m in rec.matches if m.matched_bucket == "complaint"]
        assert len(complaint_matches) > 0
        assert rec.complaint_match_count > 0

    def test_mixed_temporal_and_complaint(self, service: SnippetRecallService):
        state = _make_state(job_detail="明天开工。已招满不招了")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.temporal_match_count > 0
        assert rec.complaint_match_count > 0

    def test_complaint_only_no_recall(self, service: SnippetRecallService):
        """仅有 complaint 命中时 has_recall 应为 False（仅 temporal 命中才算）。"""
        state = _make_state(job_detail="已招满，不再招人")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        # 如果只有 complaint 命中，has_recall 应为 False
        if rec.temporal_match_count == 0:
            assert rec.has_recall is False


class TestDeduplication:
    """同一来源同一桶内的 terms 去重。"""

    def test_terms_deduplicated(self, service: SnippetRecallService):
        # 同一句中多个词属于同一桶，terms 应去重
        state = _make_state(job_detail="今天明天都可以来")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        # 同一来源同一桶只出现一个 RuleMatch
        seen = set()
        for m in rec.matches:
            key = (m.source, m.matched_bucket)
            assert key not in seen, f"重复 RuleMatch: {key}"
            seen.add(key)


class TestMatchedSources:
    """matched_sources 正确记录有命中的来源。"""

    def test_single_source(self, service: SnippetRecallService):
        state = _make_state(job_detail="明天开工")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert "job_detail" in rec.matched_sources

    def test_multiple_sources(self, service: SnippetRecallService):
        state = _make_state(job_detail="明天开工", im_text="后天来上班")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert "job_detail" in rec.matched_sources
        assert "im_text" in rec.matched_sources


class TestEmptyInput:
    """空文本输入应返回 has_recall=False。"""

    def test_all_empty(self, service: SnippetRecallService):
        state = _make_state()
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.has_recall is False
        assert rec.temporal_match_count == 0
        assert rec.complaint_match_count == 0
        assert len(rec.matches) == 0
        assert len(rec.matched_sources) == 0

    def test_no_match_text(self, service: SnippetRecallService):
        state = _make_state(job_detail="这是一段完全无关的文字描述")
        result = service.run(state)
        rec = result.snippet_recall_record
        assert rec is not None
        assert rec.has_recall is False
