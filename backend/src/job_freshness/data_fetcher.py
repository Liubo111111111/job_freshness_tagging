"""
数据获取模块 — 从 ODPS 读取 ads_freshness_candidates 宽表数据。

宽表由调度任务（fetch_freshness_candidates.sql）定期写入。
流水线运行时通过 read_freshness_candidates.sql 直接读取已聚合的数据。

重试策略: 3 次重试，5 秒间隔（网络/连接类错误）。
"""
from __future__ import annotations

import json
import logging
import socket
import time
from pathlib import Path
from typing import Any

from job_freshness.settings import load_odps_settings
from job_freshness.sql_template import load_sql_template, render_sql

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

# 读取 SQL（流水线运行时用，直接读宽表）
_READ_SQL_PATH = str(
    Path(__file__).resolve().parents[2] / "sql" / "read_freshness_candidates.sql"
)

# 建表 SQL（调度任务用，含完整 CTE 聚合逻辑）
_BUILD_SQL_PATH = str(
    Path(__file__).resolve().parents[2] / "sql" / "fetch_freshness_candidates.sql"
)

_MAX_RETRIES = 3
_RETRY_DELAY_SEC = 5


# ---------------------------------------------------------------------------
# ODPS 客户端
# ---------------------------------------------------------------------------

def _get_odps_client():
    """创建并返回 ODPS 客户端实例。"""
    from odps import ODPS  # 延迟导入，避免未安装 PyODPS 时模块加载失败

    settings = load_odps_settings()
    if not settings.access_key_id or not settings.access_key_secret:
        raise RuntimeError(
            "ODPS 配置不完整，请设置 ODPS_ACCESS_KEY_ID 和 ODPS_ACCESS_KEY_SECRET"
        )
    return ODPS(
        settings.access_key_id,
        settings.access_key_secret,
        settings.project,
        endpoint=settings.endpoint,
    )


# ---------------------------------------------------------------------------
# 核心查询函数
# ---------------------------------------------------------------------------

def _execute_with_retry(
    sql: str,
    *,
    max_retries: int = _MAX_RETRIES,
    retry_delay: int = _RETRY_DELAY_SEC,
) -> list[dict[str, Any]]:
    """执行 ODPS SQL 查询，网络错误时按策略重试。

    Args:
        sql: 已渲染的 SQL 语句
        max_retries: 最大重试次数（默认 3）
        retry_delay: 重试间隔秒数（默认 5）

    Returns:
        查询结果行列表，每行为 dict。

    Raises:
        RuntimeError: 所有重试均失败时抛出。
    """
    odps = _get_odps_client()
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.debug("执行 ODPS SQL 查询 (尝试 %d/%d)", attempt, max_retries)
            rows: list[dict[str, Any]] = []
            with odps.execute_sql(sql).open_reader() as reader:
                for record in reader:
                    row = {
                        col.name: record.get_by_name(col.name)
                        for col in record._columns
                    }
                    rows.append(row)
            logger.info("SQL 查询成功，返回 %d 条记录", len(rows))
            return rows

        except (ConnectionError, socket.gaierror, OSError) as exc:
            last_error = exc
            logger.warning(
                "网络错误 (尝试 %d/%d): %s: %s",
                attempt, max_retries, type(exc).__name__, exc,
            )
            if attempt < max_retries:
                logger.info("等待 %d 秒后重试…", retry_delay)
                time.sleep(retry_delay)

        except Exception as exc:
            last_error = exc
            err_msg = str(exc).lower()
            # 网络相关错误也重试
            if "connection" in err_msg or "name resolution" in err_msg:
                logger.warning(
                    "疑似网络错误 (尝试 %d/%d): %s",
                    attempt, max_retries, exc,
                )
                if attempt < max_retries:
                    logger.info("等待 %d 秒后重试…", retry_delay)
                    time.sleep(retry_delay)
            else:
                logger.error("非网络错误，停止重试: %s", exc)
                break

    raise RuntimeError(f"ODPS 查询在 {max_retries} 次重试后仍失败: {last_error}")


def fetch_by_sql(sql: str) -> list[dict[str, Any]]:
    """执行已渲染的 SQL 并返回原始行列表。

    供 batch_scheduler 等上层模块直接调用（SQL 已由调用方渲染）。
    """
    return _execute_with_retry(sql)


def fetch_freshness_candidates(
    bizdate: str,
    *,
    max_rows: int | None = None,
) -> list[dict[str, Any]]:
    """从 ads_freshness_candidates 宽表读取已聚合的候选数据。

    Args:
        bizdate: 分区日期，格式 yyyymmdd
        max_rows: 可选行数限制

    Returns:
        原始查询结果行列表。
    """
    template = load_sql_template(_READ_SQL_PATH)
    sql = render_sql(template, bizdate, max_rows=max_rows)
    logger.info("读取宽表数据: bizdate=%s, max_rows=%s", bizdate, max_rows)
    return _execute_with_retry(sql)


def fetch_and_convert(
    pt: str,
    output_dir: str | Path = "output/data",
    format: str = "json",
    max_rows: int | None = None,
) -> Path:
    """拉取数据并保存为 JSON 文件，供 CLI fetch / fetch-run 模式使用。

    Args:
        pt: 分区日期 yyyymmdd
        output_dir: 输出目录
        format: 输出格式（目前仅支持 json）
        max_rows: 可选行数限制

    Returns:
        写入的文件路径。
    """
    rows = fetch_freshness_candidates(pt, max_rows=max_rows)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    file_path = out / f"freshness_candidates_{pt}.json"
    file_path.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("数据已保存: %s (%d 条)", file_path, len(rows))
    return file_path
