"""端到端 dry-run 集成测试 — 使用 mock LLM 验证完整流水线。

验证：
- WideRow → snippet_recall → signal_detection → time_normalization → risk_assess → final_decision → writer output
- formal 和 fallback 路由
- JSONL + SQLite 双写
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pytest

from job_freshness.graph_state import GraphState
from job_freshness.nodes.snippet_recall import SnippetRecallService
from job_freshness.nodes.text_cleaning import TextCleaningService
from job_freshness.nodes.signal_detection import SignalDetectionService
from job_freshness.nodes.time_normalization import TimeNormalizationService
from job_freshness.nodes.risk_assess import RiskAssessService
from job_freshness.nodes.final_decision import FinalDecisionService
from job_freshness.schemas import WideRow
from job_freshness.writers.formal_output import FormalOutputWriter
from job_freshness.writers.fallback_output import FallbackOutputWriter
from job_freshness.writers.jsonl_store import JsonlKeyedStore
from job_freshness.writers.sqlite_store import SqliteResultStore

# ---------------------------------------------------------------------------
# 测试数据加载
# ---------------------------------------------------------------------------

_FIXTURES_DIR = Path(__file__).parent / "fixtures"
_LEXICON_PATH = Path(__file__).resolve().parents[3] / "config" / "recall_lexicon_v1.json"


def _load_fixture(name: str) -> Any:
    path = _FIXTURES_DIR / name
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Mock LLM Client
# ---------------------------------------------------------------------------


class MockLLMClient:
    """根据 prompt 中的 [TASK: xxx] 标记返回预设 JSON 响应。"""

    def __init__(self, responses: dict[str, dict[str, Any]]) -> None:
        self._responses = responses

    def complete(self, prompt: str, payload: dict) -> str:
        """检测 prompt 中的 task 标记，返回对应的 JSON 响应。"""
        node_name = self._detect_node(prompt)
        if node_name not in self._responses:
            raise ValueError(f"No mock response for node: {node_name}")
        return json.dumps(self._responses[node_name], ensure_ascii=False)

    @staticmethod
    def _detect_node(prompt: str) -> str:
        """从 prompt 中提取 [TASK: xxx] 标记。"""
        if "[TASK: signal_detection]" in prompt:
            return "signal_detection"
        elif "[TASK: time_normalization]" in prompt:
            return "time_normalization"
        elif "[TASK: risk_assess]" in prompt:
            return "risk_assess"
        elif "[TASK: final_decision]" in prompt:
            return "final_decision"
        raise ValueError(f"Cannot detect node from prompt: {prompt[:100]}")



# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def _build_initial_state(wide_row_dict: dict[str, Any]) -> GraphState:
    """从 WideRow fixture 构建初始 GraphState。"""
    # 移除 _scenario 描述字段
    row_data = {k: v for k, v in wide_row_dict.items() if not k.startswith("_")}
    wide_row = WideRow.model_validate(row_data)

    return GraphState(
        run_id=str(uuid.uuid4()),
        entity_key=wide_row.info_id,
        feature_schema_version="1.0",
        graph_version="1.0",
        prompt_version_detection="v1",
        prompt_version_normalization="v1",
        prompt_version_risk="v1",
        prompt_version_final="v1",
        model_version_detection="mock-model",
        model_version_normalization="mock-model",
        model_version_risk="mock-model",
        model_version_final="mock-model",
        wide_row=wide_row,
    )


def _run_pipeline(
    state: GraphState,
    mock_client: MockLLMClient,
) -> GraphState:
    """手动执行完整流水线（不使用 LangGraph 图引擎，避免 settings 依赖）。

    执行顺序：text_cleaning → snippet_recall → signal_detection + risk_assess → time_normalization(条件) → final_decision
    """
    # Step 0: text_cleaning（纯规则）
    cleaning_service = TextCleaningService()
    state = cleaning_service.run(state)

    # Step 1: snippet_recall（纯规则）
    recall_service = SnippetRecallService(lexicon_path=_LEXICON_PATH)
    state = recall_service.run(state)

    # Step 2: signal_detection
    detection_service = SignalDetectionService(
        client=mock_client,
        model_version=state.model_version_detection,
        prompt_version=state.prompt_version_detection,
    )
    state = detection_service.run(state)

    # 如果 signal_detection 出错，提前返回
    if state.error_type is not None:
        return state

    # Step 3: risk_assess（并行，但测试中顺序执行）
    risk_service = RiskAssessService(
        client=mock_client,
        model_version=state.model_version_risk,
        prompt_version=state.prompt_version_risk,
    )
    state = risk_service.run(state)

    # 如果 risk_assess 出错，提前返回
    if state.error_type is not None:
        return state

    # Step 4: time_normalization（条件执行）
    if state.signal_detection_record and state.signal_detection_record.has_temporal_signal:
        norm_service = TimeNormalizationService(
            client=mock_client,
            model_version=state.model_version_normalization,
            prompt_version=state.prompt_version_normalization,
        )
        state = norm_service.run(state)
    else:
        # 跳过 normalization，构造默认 TemporalSignalRecord
        norm_service = TimeNormalizationService(
            client=mock_client,
            model_version=state.model_version_normalization,
            prompt_version=state.prompt_version_normalization,
        )
        state = norm_service.run(state)

    # 如果 time_normalization 出错，提前返回
    if state.error_type is not None:
        return state

    # Step 5: final_decision
    decision_service = FinalDecisionService(
        client=mock_client,
        model_version=state.model_version_final,
        prompt_version=state.prompt_version_final,
    )
    state = decision_service.run(state)

    return state


# ---------------------------------------------------------------------------
# 测试用例
# ---------------------------------------------------------------------------


class TestFormalRoute:
    """验证走 formal 路由的场景（has_signal, no_signal）。"""

    def test_has_signal_formal_route(self, tmp_path: Path) -> None:
        """has_signal 场景：高置信度，走 formal 路由，双写验证。"""
        wide_rows = _load_fixture("wide_rows.json")
        llm_responses = _load_fixture("llm_responses.json")

        # 使用 has_signal 场景
        wide_row_dict = wide_rows[0]
        mock_client = MockLLMClient(llm_responses["has_signal"])

        state = _build_initial_state(wide_row_dict)
        state = _run_pipeline(state, mock_client)

        # 验证路由
        assert state.route == "formal"
        assert state.error_type is None
        assert state.decision_record is not None
        assert state.decision_record.low_confidence is False

        # 验证 temporal_signal_record 已填充
        assert state.temporal_signal_record is not None
        assert state.temporal_signal_record.has_temporal_signal is True
        assert state.temporal_signal_record.work_start_at == "2026-04-21T14:00:00+08:00"

        # 验证双写
        self._verify_dual_write_formal(state, tmp_path)

    def test_no_signal_formal_route(self, tmp_path: Path) -> None:
        """no_signal 场景：无时效信号但高置信度，走 formal 路由。"""
        wide_rows = _load_fixture("wide_rows.json")
        llm_responses = _load_fixture("llm_responses.json")

        wide_row_dict = wide_rows[1]
        mock_client = MockLLMClient(llm_responses["no_signal"])

        state = _build_initial_state(wide_row_dict)
        state = _run_pipeline(state, mock_client)

        # 验证路由
        assert state.route == "formal"
        assert state.error_type is None
        assert state.decision_record is not None
        assert state.decision_record.low_confidence is False
        assert state.decision_record.temporal_status == "no_signal"

        # 验证 temporal_signal_record（跳过 normalization）
        assert state.temporal_signal_record is not None
        assert state.temporal_signal_record.has_temporal_signal is False
        assert state.temporal_signal_record.work_start_at is None

        # 验证双写
        self._verify_dual_write_formal(state, tmp_path)

    def _verify_dual_write_formal(self, state: GraphState, tmp_path: Path) -> None:
        """验证 formal 路由的 JSONL + SQLite 双写。"""
        jsonl_path = tmp_path / "formal_output.jsonl"
        sqlite_path = tmp_path / "pipeline_results.sqlite3"

        jsonl_store = JsonlKeyedStore(jsonl_path)
        sqlite_store = SqliteResultStore(sqlite_path)

        try:
            writer = FormalOutputWriter(jsonl_store=jsonl_store, sqlite_store=sqlite_store)
            writer.run(state)

            # 验证 JSONL 写入
            assert jsonl_path.exists()
            lines = jsonl_path.read_text(encoding="utf-8").strip().split("\n")
            assert len(lines) == 1
            record = json.loads(lines[0])
            assert record["info_id"] == state.entity_key
            assert "temporal_status" in record
            assert "audit" in record

            # 验证 SQLite 写入
            conn = sqlite3.connect(sqlite_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM pipeline_runs WHERE run_id = ?", (state.run_id,)
            ).fetchone()
            assert row is not None
            assert row["entity_key"] == state.entity_key
            assert row["route"] == "formal"

            pub = conn.execute(
                "SELECT * FROM published_records WHERE run_id = ?", (state.run_id,)
            ).fetchone()
            assert pub is not None
            assert pub["route"] == "formal"
            conn.close()
        finally:
            sqlite_store.close()



class TestFallbackRoute:
    """验证走 fallback 路由的场景（conflict, cannot_determine）。"""

    def test_conflict_fallback_route(self, tmp_path: Path) -> None:
        """conflict 场景：时间信号冲突，低置信度，走 fallback 路由。"""
        wide_rows = _load_fixture("wide_rows.json")
        llm_responses = _load_fixture("llm_responses.json")

        wide_row_dict = wide_rows[2]
        mock_client = MockLLMClient(llm_responses["conflict"])

        state = _build_initial_state(wide_row_dict)
        state = _run_pipeline(state, mock_client)

        # 验证路由
        assert state.route == "fallback"
        assert state.decision_record is not None
        assert state.decision_record.low_confidence is True
        assert state.decision_record.temporal_status == "conflict"

        # 验证 risk_record
        assert state.risk_record is not None
        assert state.risk_record.stale_risk_hint is True
        assert state.risk_record.complaint_risk_hint.has_complaint_signal is True

        # 验证双写
        self._verify_dual_write_fallback(state, tmp_path)

    def test_cannot_determine_fallback_route(self, tmp_path: Path) -> None:
        """cannot_determine 场景：模糊信号，低置信度，走 fallback 路由。"""
        wide_rows = _load_fixture("wide_rows.json")
        llm_responses = _load_fixture("llm_responses.json")

        wide_row_dict = wide_rows[3]
        mock_client = MockLLMClient(llm_responses["cannot_determine"])

        state = _build_initial_state(wide_row_dict)
        state = _run_pipeline(state, mock_client)

        # 验证路由
        assert state.route == "fallback"
        assert state.decision_record is not None
        assert state.decision_record.low_confidence is True
        assert state.decision_record.temporal_status == "cannot_determine"

        # 验证双写
        self._verify_dual_write_fallback(state, tmp_path)

    def _verify_dual_write_fallback(self, state: GraphState, tmp_path: Path) -> None:
        """验证 fallback 路由的 JSONL + SQLite 双写。"""
        jsonl_path = tmp_path / "fallback_output.jsonl"
        sqlite_path = tmp_path / "pipeline_results.sqlite3"

        jsonl_store = JsonlKeyedStore(jsonl_path)
        sqlite_store = SqliteResultStore(sqlite_path)

        try:
            writer = FallbackOutputWriter(jsonl_store=jsonl_store, sqlite_store=sqlite_store)
            writer.run(state)

            # 验证 JSONL 写入
            assert jsonl_path.exists()
            lines = jsonl_path.read_text(encoding="utf-8").strip().split("\n")
            assert len(lines) == 1
            record = json.loads(lines[0])
            assert record["info_id"] == state.entity_key
            assert "error_type" in record
            assert "decision_record" in record
            assert "audit" in record

            # 验证 SQLite 写入
            conn = sqlite3.connect(sqlite_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM pipeline_runs WHERE run_id = ?", (state.run_id,)
            ).fetchone()
            assert row is not None
            assert row["entity_key"] == state.entity_key
            assert row["route"] == "fallback"

            pub = conn.execute(
                "SELECT * FROM published_records WHERE run_id = ?", (state.run_id,)
            ).fetchone()
            assert pub is not None
            assert pub["route"] == "fallback"
            conn.close()
        finally:
            sqlite_store.close()


class TestPipelineFlow:
    """验证完整流水线流程的中间状态。"""

    def test_snippet_recall_produces_snippets_for_has_signal(self) -> None:
        """验证 snippet_recall 对含时效信号的文本产出 snippet。"""
        wide_rows = _load_fixture("wide_rows.json")
        wide_row_dict = wide_rows[0]

        state = _build_initial_state(wide_row_dict)
        recall_service = SnippetRecallService(lexicon_path=_LEXICON_PATH)
        state = recall_service.run(state)

        assert state.snippet_recall_record is not None
        assert state.snippet_recall_record.has_recall is True
        assert state.snippet_recall_record.temporal_snippet_count > 0

    def test_time_normalization_skipped_when_no_signal(self) -> None:
        """验证 no_signal 场景跳过 time_normalization LLM 调用。"""
        wide_rows = _load_fixture("wide_rows.json")
        llm_responses = _load_fixture("llm_responses.json")

        wide_row_dict = wide_rows[1]
        mock_client = MockLLMClient(llm_responses["no_signal"])

        state = _build_initial_state(wide_row_dict)

        # snippet_recall
        recall_service = SnippetRecallService(lexicon_path=_LEXICON_PATH)
        state = recall_service.run(state)

        # signal_detection → has_temporal_signal=False
        detection_service = SignalDetectionService(
            client=mock_client,
            model_version="mock-model",
            prompt_version="v1",
        )
        state = detection_service.run(state)
        assert state.signal_detection_record is not None
        assert state.signal_detection_record.has_temporal_signal is False

        # time_normalization 应跳过 LLM，直接构造默认 TemporalSignalRecord
        norm_service = TimeNormalizationService(
            client=mock_client,
            model_version="mock-model",
            prompt_version="v1",
        )
        state = norm_service.run(state)

        assert state.temporal_signal_record is not None
        assert state.temporal_signal_record.normalizable is False
        assert state.temporal_signal_record.work_start_at is None
        assert state.temporal_signal_record.recruitment_valid_until is None
        assert state.temporal_signal_record.duration_hours is None

    def test_parse_error_routes_to_fallback(self, tmp_path: Path) -> None:
        """验证 LLM 返回无效 JSON 时走 fallback 路由。"""
        wide_rows = _load_fixture("wide_rows.json")
        wide_row_dict = wide_rows[0]

        # 构造一个返回无效 JSON 的 mock client
        class BadLLMClient:
            def complete(self, prompt: str, payload: dict) -> str:
                return "这不是有效的 JSON 响应"

        state = _build_initial_state(wide_row_dict)
        mock_client = BadLLMClient()

        # snippet_recall
        recall_service = SnippetRecallService(lexicon_path=_LEXICON_PATH)
        state = recall_service.run(state)

        # signal_detection 应该解析失败
        detection_service = SignalDetectionService(
            client=mock_client,
            model_version="mock-model",
            prompt_version="v1",
        )
        state = detection_service.run(state)

        # 应该设置 error_type 并路由到 fallback
        assert state.error_type is not None
        assert state.route == "fallback"

        # 验证 fallback 双写
        jsonl_path = tmp_path / "fallback_output.jsonl"
        sqlite_path = tmp_path / "pipeline_results.sqlite3"
        jsonl_store = JsonlKeyedStore(jsonl_path)
        sqlite_store = SqliteResultStore(sqlite_path)

        try:
            writer = FallbackOutputWriter(jsonl_store=jsonl_store, sqlite_store=sqlite_store)
            writer.run(state)

            assert jsonl_path.exists()
            lines = jsonl_path.read_text(encoding="utf-8").strip().split("\n")
            record = json.loads(lines[0])
            assert record["error_type"] is not None
        finally:
            sqlite_store.close()
