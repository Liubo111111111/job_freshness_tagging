"""
批量调度器模块
支持 single-day（单天）和 multi-day（15天）两种调度模式。
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from job_freshness.main import run_batch
from job_freshness.rate_limit import RuntimeConfig
from job_freshness.sql_template import (
    load_sql_template,
    render_sql,
    validate_sql_template,
)
from job_freshness.writers.jsonl_store import JsonlKeyedStore
from job_freshness.writers.sqlite_store import SqliteResultStore

logger = logging.getLogger(__name__)

_DATE_FORMAT = "%Y%m%d"
_MAX_DATE_RANGE_DAYS = 30


class ScheduleMode(str, Enum):
    SINGLE_DAY = "single-day"
    MULTI_DAY = "multi-day"


@dataclass
class BatchScheduleConfig:
    schedule_mode: ScheduleMode
    pt: str | None = None
    pt_start: str | None = None
    pt_end: str | None = None
    sql_template: str | None = None
    output_dir: str = "output"
    max_rows: int | None = None
    worker_count: int = 4
    provider_rate_limit_per_minute: int = 120
    timeout_seconds: int = 30
    retry_limit: int = 1
    max_in_flight: int = 8


@dataclass
class DayResult:
    pt: str
    success: bool
    enterprise_count: int = 0
    skipped_count: int = 0
    formal_count: int = 0
    fallback_count: int = 0
    error_message: str | None = None


@dataclass
class BatchScheduleResult:
    schedule_mode: str
    total_days: int
    success_days: int
    failed_days: int
    day_results: list[DayResult] = field(default_factory=list)
    elapsed_seconds: float = 0.0


def _parse_date(date_str: str) -> datetime:
    """解析 yyyymmdd 格式日期字符串，失败抛出 ValueError。"""
    try:
        return datetime.strptime(date_str, _DATE_FORMAT)
    except ValueError:
        raise ValueError(
            f"日期格式无效: '{date_str}'，要求 yyyymmdd 格式"
        )


class BatchScheduler:
    """批量调度器：根据配置执行单天或多天的分类流水线。"""

    def __init__(self, config: BatchScheduleConfig) -> None:
        self.config = config

    def validate_params(self) -> None:
        """校验调度参数完整性、日期格式和日期范围。"""
        mode = self.config.schedule_mode

        if not self.config.pt:
            raise ValueError("必须提供 --pt 参数（宽表分区日期）")
        _parse_date(self.config.pt)

        if mode == ScheduleMode.MULTI_DAY:
            if not self.config.pt_start:
                raise ValueError(
                    "multi-day 模式必须提供 --pt-start 参数（发布时间起始）"
                )
            if not self.config.pt_end:
                raise ValueError(
                    "multi-day 模式必须提供 --pt-end 参数（发布时间结束）"
                )
            start_dt = _parse_date(self.config.pt_start)
            end_dt = _parse_date(self.config.pt_end)

            if start_dt > end_dt:
                raise ValueError(
                    f"--pt-start ({self.config.pt_start}) 不能晚于 "
                    f"--pt-end ({self.config.pt_end})"
                )
            delta_days = (end_dt - start_dt).days
            if delta_days > _MAX_DATE_RANGE_DAYS:
                raise ValueError(
                    f"日期范围 {delta_days} 天超过上限 {_MAX_DATE_RANGE_DAYS} 天，"
                    f"请缩小日期范围"
                )

    @staticmethod
    def _generate_date_range(start: str, end: str) -> list[str]:
        """生成从 start 到 end（含）的 yyyymmdd 日期列表。"""
        start_dt = _parse_date(start)
        end_dt = _parse_date(end)
        days = (end_dt - start_dt).days + 1
        return [
            (start_dt + timedelta(days=i)).strftime(_DATE_FORMAT)
            for i in range(days)
        ]

    def _run_single_day(self, pt: str, publish_start: str | None = None, publish_end: str | None = None, output_name: str | None = None) -> DayResult:
        """执行处理：SQL 渲染 → ODPS 查询 → 企业去重 → run_batch。

        Args:
            pt: 宽表分区日期（yyyymmdd）
            publish_start: 发布时间范围起始（yyyymmdd），默认 = pt 当天
            publish_end: 发布时间范围结束（yyyymmdd），默认 = pt 当天
            output_name: 输出子目录名，默认 = pt
        """
        ps_label = publish_start or pt
        pe_label = publish_end or pt
        logger.info("开始处理 pt=%s, 发布时间范围 %s ~ %s", pt, ps_label, pe_label)

        # 1. 加载并渲染 SQL 模板
        template = load_sql_template(self.config.sql_template)
        validate_sql_template(template)
        sql = render_sql(
            template, pt,
            max_rows=self.config.max_rows,
            publish_start=publish_start,
            publish_end=publish_end,
        )

        # 2. 通过 ODPS 执行 SQL 拉取数据
        from job_freshness.data_fetcher import fetch_by_sql

        raw_rows = fetch_by_sql(sql)

        # 3. 空数据处理
        if not raw_rows:
            logger.warning("pt=%s 查询结果为空，跳过处理", pt)
            return DayResult(pt=pt, success=True, enterprise_count=0)

        # 4. fetch_by_sql 已返回 WideRow 格式，直接使用
        wide_rows = raw_rows

        # 5. 设置输出目录
        dir_name = output_name or pt
        day_output_dir = Path(self.config.output_dir) / dir_name
        day_output_dir.mkdir(parents=True, exist_ok=True)

        # 5.1 企业级去重：跳过该分区中已处理过的企业
        existing_keys: set[str] = set()
        sqlite_path = day_output_dir / "pipeline_results.sqlite3"
        if sqlite_path.is_file():
            try:
                import sqlite3
                conn = sqlite3.connect(str(sqlite_path))
                rows = conn.execute("SELECT DISTINCT entity_key FROM pipeline_runs").fetchall()
                existing_keys = {r[0] for r in rows}
                conn.close()
            except Exception:
                logger.debug("读取已有 entity_key 失败，将全量处理", exc_info=True)

        skipped_count = 0
        if existing_keys:
            before = len(wide_rows)
            wide_rows = [r for r in wide_rows if r.get("social_credit_code", "") not in existing_keys]
            skipped_count = before - len(wide_rows)
            if skipped_count:
                logger.info("pt=%s 跳过 %d 家已处理企业（命中历史缓存），剩余 %d 家待处理", pt, skipped_count, len(wide_rows))

        if not wide_rows:
            logger.info("pt=%s 所有企业均已处理（命中历史缓存 %d 家），无需重跑", pt, skipped_count)
            # 仍然同步已有结果到 MC
            try:
                from job_freshness.writers.mc_writer import sync_to_mc
                mc_result = sync_to_mc(
                    output_dir=day_output_dir,
                    pt=pt,
                    schedule_mode=self.config.schedule_mode.value,
                )
                logger.info("MC 同步: 成功 %d, 失败 %d", mc_result.get("success", 0), mc_result.get("failed", 0))
            except Exception as exc:
                logger.error("MC 同步失败: %s", exc)
            return DayResult(pt=pt, success=True, enterprise_count=0, skipped_count=skipped_count)

        # 6. 创建输出 store
        formal_store = JsonlKeyedStore(day_output_dir / "formal_output.jsonl")
        fallback_store = JsonlKeyedStore(day_output_dir / "fallback_output.jsonl")
        sqlite_store = SqliteResultStore(
            day_output_dir / "pipeline_results.sqlite3"
        )

        # 7. 创建 LLM 客户端
        from job_freshness.llm.client import HttpLLMClient

        llm_client = HttpLLMClient()

        def _client_factory(row_dict: dict[str, Any]) -> HttpLLMClient:
            return llm_client

        # 8. 构建运行时配置
        runtime_config = RuntimeConfig(
            worker_count=self.config.worker_count,
            provider_rate_limit_per_minute=self.config.provider_rate_limit_per_minute,
            max_in_flight=self.config.max_in_flight,
            timeout_seconds=self.config.timeout_seconds,
            retry_limit=self.config.retry_limit,
        )

        # 9. 执行分类流水线（start_idx 从已有记录数+1 开始，避免 run_id 冲突）
        start_idx = len(existing_keys) + 1
        try:
            summary = run_batch(
                rows=wide_rows,
                pt=pt,
                client_factory=_client_factory,
                formal_store=formal_store,
                fallback_store=fallback_store,
                sqlite_store=sqlite_store,
                runtime_config=runtime_config,
                start_idx=start_idx,
            )
        finally:
            sqlite_store.close()
            llm_client.close()

        # 10. 写入运行摘要
        summary["skipped_count"] = skipped_count
        summary["new_enterprise_count"] = len(wide_rows)
        (day_output_dir / "run_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # 本次实际处理的企业数 = processed_count（run_batch 内部统计）
        # formal/fallback count 从 summary 取的是累积值，需要用本次处理数推算
        processed = summary.get("processed_count", len(wide_rows))

        logger.info(
            "pt=%s 处理完成: 新增 %d 家, 跳过 %d 家（命中缓存）, formal=%d, fallback=%d（累积）",
            pt, len(wide_rows), skipped_count,
            summary.get("formal_count", 0), summary.get("fallback_count", 0),
        )

        # 11. 同步分类结果到 MaxCompute
        try:
            from job_freshness.writers.mc_writer import sync_to_mc
            mc_result = sync_to_mc(
                output_dir=day_output_dir,
                pt=pt,
                schedule_mode=self.config.schedule_mode.value,
            )
            summary["mc_sync"] = mc_result
            logger.info("MC 同步: 成功 %d, 失败 %d", mc_result.get("success", 0), mc_result.get("failed", 0))
        except Exception as exc:
            logger.error("MC 同步失败: %s", exc)
            summary["mc_sync"] = {"error": str(exc)}

        # 更新运行摘要（含 MC 同步结果）
        (day_output_dir / "run_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return DayResult(
            pt=pt,
            success=True,
            enterprise_count=len(wide_rows),
            skipped_count=skipped_count,
            formal_count=summary.get("formal_count", 0),
            fallback_count=summary.get("fallback_count", 0),
        )

    def _run_multi_day(self) -> BatchScheduleResult:
        """用指定分区 + 发布时间范围执行一次查询。"""
        assert self.config.pt is not None
        assert self.config.pt_start is not None
        assert self.config.pt_end is not None

        pt = self.config.pt
        start_time = time.monotonic()

        try:
            # 输出目录用发布日期范围命名，如 20260410_20260412
            output_name = f"{self.config.pt_start}_{self.config.pt_end}"
            result = self._run_single_day(
                pt=pt,
                publish_start=self.config.pt_start,
                publish_end=self.config.pt_end,
                output_name=output_name,
            )
            day_results = [result]
            success_days = 1 if result.success else 0
            failed_days = 0 if result.success else 1
        except Exception as exc:
            logger.error("multi-day 处理失败: %s", exc)
            day_results = [
                DayResult(
                    pt=pt,
                    success=False,
                    error_message=str(exc),
                )
            ]
            success_days = 0
            failed_days = 1

        elapsed = time.monotonic() - start_time

        batch_result = BatchScheduleResult(
            schedule_mode=ScheduleMode.MULTI_DAY.value,
            total_days=1,
            success_days=success_days,
            failed_days=failed_days,
            day_results=day_results,
            elapsed_seconds=round(elapsed, 2),
        )

        # 写入 batch_summary.json
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "batch_summary.json").write_text(
            json.dumps(asdict(batch_result), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        logger.info(
            "multi-day 完成: pt=%s, 发布范围 %s~%s, 耗时 %.1fs",
            pt, self.config.pt_start, self.config.pt_end, elapsed,
        )
        return batch_result

    def run(self) -> BatchScheduleResult:
        """入口方法：校验参数后根据模式执行调度。"""
        self.validate_params()

        # 加载并校验 SQL 模板（提前失败）
        template = load_sql_template(self.config.sql_template)
        validate_sql_template(template)

        start_time = time.monotonic()

        if self.config.schedule_mode == ScheduleMode.SINGLE_DAY:
            assert self.config.pt is not None
            try:
                day_result = self._run_single_day(self.config.pt)
                day_results = [day_result]
                success_days = 1 if day_result.success else 0
                failed_days = 0 if day_result.success else 1
            except Exception as exc:
                day_results = [
                    DayResult(
                        pt=self.config.pt,
                        success=False,
                        error_message=str(exc),
                    )
                ]
                success_days = 0
                failed_days = 1

            elapsed = time.monotonic() - start_time
            return BatchScheduleResult(
                schedule_mode=ScheduleMode.SINGLE_DAY.value,
                total_days=1,
                success_days=success_days,
                failed_days=failed_days,
                day_results=day_results,
                elapsed_seconds=round(elapsed, 2),
            )

        elif self.config.schedule_mode == ScheduleMode.MULTI_DAY:
            return self._run_multi_day()

        raise ValueError(f"不支持的调度模式: {self.config.schedule_mode}")
