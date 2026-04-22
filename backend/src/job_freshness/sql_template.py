"""
SQL 模板渲染器模块
支持 ${bizdate}、${publish_start}、${publish_end} 占位符替换，可选追加 LIMIT 子句。

- ${bizdate}: 宽表分区日期（yyyymmdd），通常取最新可用分区
- ${publish_start}: 发布时间范围起始（yyyy-mm-dd）
- ${publish_end}: 发布时间范围结束（yyyy-mm-dd），不含当天
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_SQL_TEMPLATE = """\
-- 默认模板已弃用，请使用 backend/sql/fetch_freshness_candidates.sql
-- 通过 load_sql_template('backend/sql/fetch_freshness_candidates.sql') 加载
SELECT 1 WHERE 1=0"""


def load_sql_template(path: str | None) -> str:
    """加载 SQL 模板：指定路径则读取文件，否则返回默认模板。"""
    if path is None:
        logger.debug("使用默认 SQL 模板")
        return DEFAULT_SQL_TEMPLATE
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"SQL 模板文件不存在: {path}")
    content = p.read_text(encoding="utf-8")
    logger.info("已加载自定义 SQL 模板: %s", path)
    return content


def validate_sql_template(template: str) -> None:
    """校验 SQL 模板：非空且包含 ${bizdate} 占位符。"""
    if not template or not template.strip():
        raise ValueError("SQL 模板内容不能为空")
    if "${bizdate}" not in template:
        raise ValueError("SQL 模板必须包含 ${bizdate} 占位符")


def _pt_to_date(pt: str) -> str:
    """yyyymmdd → yyyy-mm-dd"""
    return f"{pt[:4]}-{pt[4:6]}-{pt[6:8]}"


def _next_day(pt: str) -> str:
    """yyyymmdd → 下一天的 yyyy-mm-dd"""
    dt = datetime.strptime(pt, "%Y%m%d") + timedelta(days=1)
    return dt.strftime("%Y-%m-%d")


def render_sql(
    template: str,
    bizdate: str,
    max_rows: int | None = None,
    publish_start: str | None = None,
    publish_end: str | None = None,
) -> str:
    """渲染 SQL 模板。

    Args:
        template: SQL 模板字符串
        bizdate: 宽表分区日期（yyyymmdd）
        max_rows: 可选 LIMIT
        publish_start: 发布时间起始（yyyymmdd），默认 = bizdate 当天
        publish_end: 发布时间结束（yyyymmdd），默认 = bizdate 下一天（不含）
    """
    # 默认：单天查询，publish_start = bizdate 当天，publish_end = bizdate+1
    ps = _pt_to_date(publish_start) if publish_start else _pt_to_date(bizdate)
    pe = _next_day(publish_end) if publish_end else _next_day(bizdate)

    rendered = (
        template
        .replace("${bizdate}", bizdate)
        .replace("${publish_start}", ps)
        .replace("${publish_end}", pe)
    )
    if max_rows is not None:
        rendered += f"\nLIMIT {max_rows}"
    return rendered
