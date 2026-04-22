"""
数据加载与归一化模块 — 将 ODPS SQL 查询结果行转换为 WideRow 实例。

职责:
- 接收原始 SQL 结果行（list[dict]）
- 归一化列名映射（SQL 列名 → WideRow 字段名）
- 处理缺失/null 的可选文本字段（按 WideRow schema 默认值填充）
- 通过 WideRow.model_validate() 校验每行
- 校验失败时记录描述性错误并跳过该行，继续处理剩余行
"""
from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from job_freshness.schemas import WideRow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL 列名 → WideRow 字段名映射
# ---------------------------------------------------------------------------
# SQL 查询输出的列名可能与 WideRow 字段名不完全一致，此映射处理差异。
# 当前 fetch_freshness_candidates.sql 已使用 AS 别名对齐，但保留映射以应对
# 历史数据或外部数据源列名不一致的情况。

_SQL_COLUMN_MAP: dict[str, str] = {
    "id": "info_id",
    "detail": "job_detail",
    "occupations_v2": "occupation_id",
    "im_msg": "im_text",
}

# 可选文本字段 — null/缺失时按 WideRow schema 默认值填充
_OPTIONAL_TEXT_FIELDS: dict[str, str | None] = {
    "asr_result": "",
    "im_text": "",
    "complaint_content": "",
    "sub_id": None,
}


# ---------------------------------------------------------------------------
# 拒绝行记录
# ---------------------------------------------------------------------------

@dataclass
class RejectedRow:
    """记录校验失败的行及其错误详情。"""

    row_index: int
    raw_row: dict[str, Any]
    error: str


# ---------------------------------------------------------------------------
# 归一化结果
# ---------------------------------------------------------------------------

@dataclass
class LoadResult:
    """归一化加载结果，包含有效行和被拒绝行。"""

    rows: list[WideRow] = field(default_factory=list)
    rejected: list[RejectedRow] = field(default_factory=list)


# ---------------------------------------------------------------------------
# 核心归一化函数
# ---------------------------------------------------------------------------

def _normalize_row(raw: dict[str, Any]) -> dict[str, Any]:
    """将原始 SQL 行字典归一化为 WideRow 字段名和默认值。

    1. 应用列名映射（_SQL_COLUMN_MAP）
    2. 对可选文本字段填充默认值（null → 空字符串或 None）
    """
    normalized: dict[str, Any] = {}
    for key, value in raw.items():
        mapped_key = _SQL_COLUMN_MAP.get(key, key)
        normalized[mapped_key] = value

    # 填充缺失或 null 的可选文本字段
    for field_name, default_value in _OPTIONAL_TEXT_FIELDS.items():
        if field_name not in normalized or normalized[field_name] is None:
            normalized[field_name] = default_value

    return normalized


def load_wide_rows(
    raw_rows: Iterable[dict[str, Any]],
) -> LoadResult:
    """将原始 SQL 结果行批量归一化并校验为 WideRow。

    Args:
        raw_rows: 原始 SQL 查询结果行（每行为 dict）。

    Returns:
        LoadResult，包含校验通过的 WideRow 列表和被拒绝行列表。
    """
    result = LoadResult()

    for idx, raw in enumerate(raw_rows):
        try:
            normalized = _normalize_row(raw)
            wide_row = WideRow.model_validate(normalized)
            result.rows.append(wide_row)
        except ValidationError as exc:
            error_msg = f"行 {idx} 校验失败: {exc}"
            logger.warning(error_msg)
            result.rejected.append(
                RejectedRow(row_index=idx, raw_row=raw, error=str(exc))
            )
        except Exception as exc:
            error_msg = f"行 {idx} 归一化异常: {type(exc).__name__}: {exc}"
            logger.warning(error_msg)
            result.rejected.append(
                RejectedRow(row_index=idx, raw_row=raw, error=str(exc))
            )

    if result.rejected:
        logger.info(
            "数据加载完成: %d 行有效, %d 行被拒绝",
            len(result.rows),
            len(result.rejected),
        )
    else:
        logger.info("数据加载完成: %d 行全部有效", len(result.rows))

    return result


# ---------------------------------------------------------------------------
# 文件加载辅助（兼容 JSON / JSONL 输入）
# ---------------------------------------------------------------------------

def load_rows_from_file(
    path: str | Path,
    pt: str | None = None,
) -> Iterator[dict[str, Any]]:
    """从 JSON 或 JSONL 文件加载原始行，可选按分区日期过滤。

    Args:
        path: 输入文件路径（.json 或 .jsonl）
        pt: 可选分区日期过滤

    Yields:
        原始行字典。
    """
    source = Path(path)
    if source.suffix.lower() == ".jsonl":
        lines = source.read_text(encoding="utf-8").splitlines()
        records: Iterable[dict[str, Any]] = (
            json.loads(line) for line in lines if line.strip()
        )
    else:
        payload = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("输入文件必须包含 JSON 数组或 JSONL 行")
        records = iter(payload)

    for record in records:
        if pt is not None and str(record.get("pt", "")) != str(pt):
            continue
        # 移除分区字段，不属于 WideRow
        filtered = {k: v for k, v in record.items() if k != "pt"}
        yield filtered
