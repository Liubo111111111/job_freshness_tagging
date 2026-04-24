"""
职位新鲜度识别流水线 CLI 入口。

支持模式：
  dry-run     — 使用内置 mock 数据验证流水线（不发起真实网络请求）
  fetch       — 从 ODPS 拉取数据并保存为 JSON/JSONL
  fetch-run   — 拉取数据 + 运行流水线一站式
  run         — 对已有 JSON/JSONL 数据文件运行流水线（批量）
  run-single  — 对已有数据文件运行流水线（仅第一条）
  schedule    — 调度模式（single-day / multi-day）
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

from job_freshness.graph_state import GraphState
from job_freshness.loader import load_rows_from_file, load_wide_rows
from job_freshness.nodes.final_decision.service import FinalDecisionService
from job_freshness.nodes.risk_assess.service import RiskAssessService
from job_freshness.nodes.signal_detection.service import SignalDetectionService
from job_freshness.nodes.snippet_recall.service import SnippetRecallService
from job_freshness.nodes.text_cleaning.service import TextCleaningService
from job_freshness.nodes.time_normalization.service import TimeNormalizationService
from job_freshness.rate_limit import MinuteRateLimiter, RuntimeConfig
from job_freshness.schemas import WideRow
from job_freshness.writers.fallback_output import FallbackOutputWriter
from job_freshness.writers.formal_output import FormalOutputWriter
from job_freshness.writers.jsonl_store import JsonlKeyedStore
from job_freshness.writers.sqlite_store import SqliteResultStore

logger = logging.getLogger(__name__)

# 词表配置路径
_LEXICON_PATH = Path(__file__).resolve().parents[3] / "config" / "recall_lexicon_v1.json"


# ---------------------------------------------------------------------------
# Mock LLM Client（dry-run 用）
# ---------------------------------------------------------------------------


class MockLLMClient:
    """根据 prompt 中的 [TASK: xxx] 标记返回预设 JSON 响应。"""

    def __init__(self, responses: dict[str, dict[str, Any]]) -> None:
        self._responses = responses
        self.calls = 0

    def complete(self, prompt: str, payload: dict) -> str:
        del payload
        self.calls += 1
        node = self._detect_node(prompt)
        if node not in self._responses:
            raise ValueError(f"mock 响应中未找到节点: {node}")
        return json.dumps(self._responses[node], ensure_ascii=False)

    @staticmethod
    def _detect_node(prompt: str) -> str:
        if "[TASK: signal_detection]" in prompt:
            return "signal_detection"
        if "[TASK: time_normalization]" in prompt:
            return "time_normalization"
        if "[TASK: risk_assess]" in prompt:
            return "risk_assess"
        if "[TASK: final_decision]" in prompt:
            return "final_decision"
        raise ValueError(f"无法从 prompt 中识别节点: {prompt[:100]}")


class RateLimitedLLMClient:
    """带限流的 LLM 客户端包装器。"""

    def __init__(self, client: Any, limiter: MinuteRateLimiter) -> None:
        self.client = client
        self.limiter = limiter

    def complete(self, prompt: str, payload: dict) -> str:
        self.limiter.acquire()
        return self.client.complete(prompt, payload)


# ---------------------------------------------------------------------------
# 构建初始 GraphState
# ---------------------------------------------------------------------------


def build_initial_state(
    wide_row: WideRow,
    run_id: str | None = None,
    feature_schema_version: str = "v1",
    graph_version: str = "v1",
    prompt_version_detection: str = "v1",
    prompt_version_normalization: str = "v1",
    prompt_version_risk: str = "v1",
    prompt_version_final: str = "v1",
    model_version_detection: str = "qwen3-max",
    model_version_normalization: str = "qwen3-max",
    model_version_risk: str = "qwen3-max",
    model_version_final: str = "qwen3-max",
) -> GraphState:
    """从 WideRow 构建初始 GraphState。"""
    return GraphState(
        run_id=run_id or str(uuid.uuid4()),
        entity_key=wide_row.info_id,
        feature_schema_version=feature_schema_version,
        graph_version=graph_version,
        prompt_version_detection=prompt_version_detection,
        prompt_version_normalization=prompt_version_normalization,
        prompt_version_risk=prompt_version_risk,
        prompt_version_final=prompt_version_final,
        model_version_detection=model_version_detection,
        model_version_normalization=model_version_normalization,
        model_version_risk=model_version_risk,
        model_version_final=model_version_final,
        wide_row=wide_row,
    )


# ---------------------------------------------------------------------------
# 单条执行
# ---------------------------------------------------------------------------


def run_once(
    wide_row: WideRow,
    run_id: str,
    client: Any,
    formal_writer: FormalOutputWriter,
    fallback_writer: FallbackOutputWriter,
    **state_kwargs: Any,
) -> GraphState:
    """执行完整流水线：text_cleaning → snippet_recall → signal_detection → risk_assess → time_normalization → final_decision → writer。"""
    state = build_initial_state(wide_row=wide_row, run_id=run_id, **state_kwargs)

    # Step 0: text_cleaning（纯规则）
    cleaning_service = TextCleaningService()
    state = cleaning_service.run(state)

    # Step 1: snippet_recall（纯规则）
    recall_service = SnippetRecallService(lexicon_path=_LEXICON_PATH)
    state = recall_service.run(state)

    # 如果没有召回任何片段，跳过所有 LLM 节点，直接输出 no_signal
    if not state.snippet_recall_record or not state.snippet_recall_record.has_recall:
        from job_freshness.schemas import (
            FreshnessDecisionRecord,
            RiskRecord,
            TemporalSignalRecord,
        )
        state = state.model_copy(update={
            "temporal_signal_record": TemporalSignalRecord(
                has_temporal_signal=False,
                temporal_status="no_signal",
                signal_type="no_signal",
                confidence=1.0,
            ),
            "risk_record": RiskRecord(),
            "decision_record": FreshnessDecisionRecord(
                validity_type="no_validity",
                reason="片段召回为空，无时效信号和风险信号，跳过 LLM 推理",
                low_confidence=False,
            ),
            "route": "formal",
        })
        return formal_writer.run(state)

    # Step 2: signal_detection
    detection_service = SignalDetectionService(
        client=client,
        model_version=state.model_version_detection,
        prompt_version=state.prompt_version_detection,
    )
    state = detection_service.run(state)
    if state.error_type is not None:
        return fallback_writer.run(state)

    # Step 3: risk_assess（纯规则，不需要 LLM）
    risk_service = RiskAssessService()
    state = risk_service.run(state)

    # Step 4: time_normalization（条件执行）
    norm_service = TimeNormalizationService(
        client=client,
        model_version=state.model_version_normalization,
        prompt_version=state.prompt_version_normalization,
    )
    state = norm_service.run(state)
    if state.error_type is not None:
        return fallback_writer.run(state)

    # Step 5: final_decision
    decision_service = FinalDecisionService(
        client=client,
        model_version=state.model_version_final,
        prompt_version=state.prompt_version_final,
    )
    state = decision_service.run(state)

    # 路由到 formal 或 fallback
    if state.route == "formal":
        return formal_writer.run(state)
    else:
        return fallback_writer.run(state)


# ---------------------------------------------------------------------------
# 批量执行
# ---------------------------------------------------------------------------


def run_batch(
    rows: list[WideRow],
    pt: str,
    client_factory: Any,
    formal_writer: FormalOutputWriter,
    fallback_writer: FallbackOutputWriter,
    runtime_config: RuntimeConfig | None = None,
    start_idx: int = 1,
) -> dict[str, Any]:
    """批量执行流水线，支持并发和限流。"""
    del pt  # 保留参数签名兼容性
    config = runtime_config or RuntimeConfig()
    limiter = MinuteRateLimiter(config.provider_rate_limit_per_minute)
    processed_count = 0
    formal_count = 0
    fallback_count = 0
    future_to_key: dict[Future[GraphState], str] = {}
    max_in_flight = max(1, min(config.max_in_flight, config.worker_count))

    def _process_one(wide_row: WideRow, idx: int) -> GraphState:
        client = RateLimitedLLMClient(client_factory(wide_row), limiter)
        return run_once(
            wide_row=wide_row,
            run_id=f"batch-{idx}",
            client=client,
            formal_writer=formal_writer,
            fallback_writer=fallback_writer,
        )

    def drain_one_or_more() -> None:
        nonlocal processed_count, formal_count, fallback_count
        done, _ = wait(
            list(future_to_key),
            timeout=config.timeout_seconds,
            return_when=FIRST_COMPLETED,
        )
        if not done:
            raise TimeoutError(f"batch_timeout_after_{config.timeout_seconds}_seconds")
        for future in done:
            state = future.result()
            del future_to_key[future]
            processed_count += 1
            if state.route == "formal":
                formal_count += 1
            else:
                fallback_count += 1

    with ThreadPoolExecutor(max_workers=config.worker_count) as executor:
        for idx, wide_row in enumerate(rows, start=start_idx):
            while len(future_to_key) >= max_in_flight:
                drain_one_or_more()

            future = executor.submit(_process_one, wide_row, idx)
            future_to_key[future] = wide_row.info_id

        while future_to_key:
            drain_one_or_more()

    return {
        "processed_count": processed_count,
        "formal_count": formal_count,
        "fallback_count": fallback_count,
    }


# ---------------------------------------------------------------------------
# Dry-Run 模式
# ---------------------------------------------------------------------------


def run_dry_run(pt: str, cases_path: str | None = None) -> dict[str, Any]:
    """使用 mock LLM 响应验证完整流水线。"""
    del pt
    # 加载 fixture
    fixtures_dir = Path(__file__).resolve().parents[2] / "tests" / "integration" / "fixtures"
    wide_rows_path = Path(cases_path) if cases_path else fixtures_dir / "wide_rows.json"
    llm_responses_path = fixtures_dir / "llm_responses.json"

    if not wide_rows_path.exists():
        raise FileNotFoundError(f"Fixture 文件不存在: {wide_rows_path}")
    if not llm_responses_path.exists():
        raise FileNotFoundError(f"Fixture 文件不存在: {llm_responses_path}")

    raw_rows = json.loads(wide_rows_path.read_text(encoding="utf-8"))
    llm_responses = json.loads(llm_responses_path.read_text(encoding="utf-8"))

    # 场景名映射
    scenario_map = {
        "JOB_HAS_SIGNAL_001": "has_signal",
        "JOB_NO_SIGNAL_002": "no_signal",
        "JOB_CONFLICT_003": "conflict",
        "JOB_CANNOT_DETERMINE_004": "cannot_determine",
    }

    formal_count = 0
    fallback_count = 0
    total_llm_calls = 0
    results: list[dict[str, Any]] = []

    for raw_row in raw_rows:
        # 移除 _scenario 描述字段
        row_data = {k: v for k, v in raw_row.items() if not k.startswith("_")}
        wide_row = WideRow.model_validate(row_data)

        # 查找对应的 mock 响应
        scenario = scenario_map.get(wide_row.info_id)
        if scenario is None or scenario not in llm_responses:
            logger.warning("跳过未知场景: info_id=%s", wide_row.info_id)
            continue

        mock_client = MockLLMClient(llm_responses[scenario])

        # 创建临时 writer（不写入文件，仅验证流程）
        state = build_initial_state(wide_row=wide_row, run_id=f"dry-run-{wide_row.info_id}")

        # 执行流水线
        # Step 0: text_cleaning（纯规则）
        cleaning_service = TextCleaningService()
        state = cleaning_service.run(state)

        recall_service = SnippetRecallService(lexicon_path=_LEXICON_PATH)
        state = recall_service.run(state)

        # 如果没有召回，跳过 LLM 节点
        if not state.snippet_recall_record or not state.snippet_recall_record.has_recall:
            from job_freshness.schemas import (
                FreshnessDecisionRecord,
                RiskRecord,
                TemporalSignalRecord,
            )
            state = state.model_copy(update={
                "temporal_signal_record": TemporalSignalRecord(
                    has_temporal_signal=False,
                    temporal_status="no_signal",
                    signal_type="no_signal",
                    confidence=1.0,
                ),
                "risk_record": RiskRecord(),
                "decision_record": FreshnessDecisionRecord(
                    validity_type="no_validity",
                    reason="片段召回为空，跳过 LLM 推理",
                    low_confidence=False,
                ),
                "route": "formal",
            })
        else:
            detection_service = SignalDetectionService(
                client=mock_client,
                model_version=state.model_version_detection,
                prompt_version=state.prompt_version_detection,
            )
            state = detection_service.run(state)

            if state.error_type is None:
                risk_service = RiskAssessService()
                state = risk_service.run(state)

            if state.error_type is None:
                norm_service = TimeNormalizationService(
                    client=mock_client,
                    model_version=state.model_version_normalization,
                    prompt_version=state.prompt_version_normalization,
                )
                state = norm_service.run(state)

            if state.error_type is None:
                decision_service = FinalDecisionService(
                    client=mock_client,
                    model_version=state.model_version_final,
                    prompt_version=state.prompt_version_final,
                )
                state = decision_service.run(state)

        total_llm_calls += mock_client.calls

        if state.route == "formal":
            formal_count += 1
        else:
            fallback_count += 1

        results.append({
            "info_id": wide_row.info_id,
            "scenario": scenario,
            "route": state.route,
            "validity_type": state.decision_record.validity_type if state.decision_record else None,
            "estimated_expiry": state.decision_record.estimated_expiry if state.decision_record else None,
            "error_type": state.error_type,
        })

    return {
        "processed_count": len(results),
        "formal_count": formal_count,
        "fallback_count": fallback_count,
        "llm_calls": total_llm_calls,
        "results": results,
    }


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """CLI 入口函数。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="职位新鲜度识别流水线")
    parser.add_argument("--pt", help="业务日期分区（yyyymmdd），默认 T-1")
    parser.add_argument(
        "--mode", required=True,
        choices=["dry-run", "fetch", "fetch-run", "run", "run-single", "schedule"],
        help="执行模式",
    )
    parser.add_argument("--cases-path", help="dry-run 自定义 fixture 路径")
    parser.add_argument("--input-path", help="JSON/JSONL 输入文件路径（run/run-single 模式）")
    parser.add_argument("--output-dir", help="输出目录", default="output")
    parser.add_argument("--format", choices=["json", "jsonl"], default="json", help="fetch 模式输出格式")
    parser.add_argument("--max-rows", type=int, default=None, help="ODPS 最大拉取条数")
    parser.add_argument("--worker-count", type=int, default=4, help="并发工作线程数")
    parser.add_argument("--provider-rate-limit-per-minute", type=int, default=120, help="LLM 调用限流（次/分钟）")
    parser.add_argument("--timeout-seconds", type=int, default=30, help="单任务超时（秒）")
    parser.add_argument("--retry-limit", type=int, default=1, help="失败重试次数")
    parser.add_argument("--max-in-flight", type=int, default=8, help="最大并发任务数")
    parser.add_argument("--schedule-mode", choices=["single-day", "multi-day"], help="调度模式")
    parser.add_argument("--pt-start", help="multi-day 模式起始日期 yyyymmdd")
    parser.add_argument("--pt-end", help="multi-day 模式结束日期 yyyymmdd")
    parser.add_argument("--sql-template", help="自定义 SQL 模板文件路径")
    args = parser.parse_args(argv)

    # --pt 默认值：昨天 (T-1)
    if not args.pt:
        from datetime import datetime, timedelta
        args.pt = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        print(f"--pt 未指定，使用默认值 T-1: {args.pt}")

    runtime_config = RuntimeConfig(
        worker_count=args.worker_count,
        provider_rate_limit_per_minute=args.provider_rate_limit_per_minute,
        timeout_seconds=args.timeout_seconds,
        retry_limit=args.retry_limit,
        max_in_flight=args.max_in_flight,
    )

    # ── dry-run ──
    if args.mode == "dry-run":
        summary = run_dry_run(pt=args.pt, cases_path=args.cases_path)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    # ── fetch ──
    if args.mode == "fetch":
        from job_freshness.data_fetcher import fetch_freshness_candidates
        rows = fetch_freshness_candidates(args.pt, max_rows=args.max_rows)
        output_dir = Path(args.output_dir) / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"freshness_candidates_{args.pt}.json"
        file_path.write_text(
            json.dumps(rows, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"数据已保存: {file_path} ({len(rows)} 条)")
        return 0

    # ── fetch-run ──
    if args.mode == "fetch-run":
        from job_freshness.data_fetcher import fetch_freshness_candidates
        from job_freshness.llm.client import HttpLLMClient

        # 1. 拉取数据
        raw_rows = fetch_freshness_candidates(args.pt, max_rows=args.max_rows)
        if not raw_rows:
            print("拉取到的数据为空，无法运行流水线。")
            return 1

        # 2. 归一化
        load_result = load_wide_rows(raw_rows)
        if not load_result.rows:
            print(f"所有 {len(raw_rows)} 行均校验失败，无法运行流水线。")
            return 1
        if load_result.rejected:
            print(f"警告: {len(load_result.rejected)} 行校验失败，已跳过。")

        # 3. 运行流水线
        output_dir = Path(args.output_dir) / args.pt
        output_dir.mkdir(parents=True, exist_ok=True)

        llm_client = HttpLLMClient()
        formal_store = JsonlKeyedStore(output_dir / "formal_output.jsonl")
        fallback_store = JsonlKeyedStore(output_dir / "fallback_output.jsonl")
        sqlite_store = SqliteResultStore(output_dir / "pipeline_results.sqlite3")

        formal_writer = FormalOutputWriter(jsonl_store=formal_store, sqlite_store=sqlite_store)
        fallback_writer = FallbackOutputWriter(jsonl_store=fallback_store, sqlite_store=sqlite_store)

        def _client_factory(wide_row: WideRow) -> Any:
            return llm_client

        try:
            summary = run_batch(
                rows=load_result.rows,
                pt=args.pt,
                client_factory=_client_factory,
                formal_writer=formal_writer,
                fallback_writer=fallback_writer,
                runtime_config=runtime_config,
            )
        finally:
            sqlite_store.close()
            llm_client.close()

        (output_dir / "run_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    # ── run / run-single ──
    if args.mode in ("run", "run-single"):
        from job_freshness.llm.client import HttpLLMClient

        if not args.input_path:
            print(f"错误: {args.mode} 模式需要 --input-path 参数", file=sys.stderr)
            return 1

        raw_rows = list(load_rows_from_file(args.input_path, pt=args.pt))
        if not raw_rows:
            print("未找到匹配的数据行。")
            return 1

        load_result = load_wide_rows(raw_rows)
        if not load_result.rows:
            print("所有行均校验失败。")
            return 1

        wide_rows = load_result.rows
        if args.mode == "run-single":
            wide_rows = wide_rows[:1]

        output_dir = Path(args.output_dir) / args.pt
        output_dir.mkdir(parents=True, exist_ok=True)

        llm_client = HttpLLMClient()
        formal_store = JsonlKeyedStore(output_dir / "formal_output.jsonl")
        fallback_store = JsonlKeyedStore(output_dir / "fallback_output.jsonl")
        sqlite_store = SqliteResultStore(output_dir / "pipeline_results.sqlite3")

        formal_writer = FormalOutputWriter(jsonl_store=formal_store, sqlite_store=sqlite_store)
        fallback_writer = FallbackOutputWriter(jsonl_store=fallback_store, sqlite_store=sqlite_store)

        def _client_factory(wide_row: WideRow) -> Any:
            return llm_client

        try:
            summary = run_batch(
                rows=wide_rows,
                pt=args.pt,
                client_factory=_client_factory,
                formal_writer=formal_writer,
                fallback_writer=fallback_writer,
                runtime_config=runtime_config,
            )
        finally:
            sqlite_store.close()
            llm_client.close()

        (output_dir / "run_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    # ── schedule ──
    if args.mode == "schedule":
        print("schedule 模式暂未适配新鲜度流水线，请使用 fetch-run 模式。", file=sys.stderr)
        return 1

    print(f"不支持的模式: {args.mode}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
