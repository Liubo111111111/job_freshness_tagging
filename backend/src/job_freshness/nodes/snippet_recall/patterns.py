"""正则模式编译和匹配工具函数。"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MatchResult:
    """单次匹配结果。"""

    term: str
    bucket: str  # 词表桶名 或 正则组名（如 date_patterns）


def find_all_matches(
    text: str,
    term_patterns: dict[str, re.Pattern[str]],
    regex_patterns: dict[str, re.Pattern[str]],
) -> list[MatchResult]:
    """在 text 中查找所有词表和正则命中，返回去重后的 MatchResult 列表。"""
    results: list[MatchResult] = []
    seen: set[tuple[str, str]] = set()

    # 词表匹配
    for bucket, pattern in term_patterns.items():
        for m in pattern.finditer(text):
            key = (m.group(), bucket)
            if key not in seen:
                seen.add(key)
                results.append(MatchResult(term=m.group(), bucket=bucket))

    # 正则模式匹配 — 正则组映射到虚拟桶名（去掉 _patterns 后缀）
    for group, pattern in regex_patterns.items():
        bucket_name = _regex_group_to_bucket(group)
        for m in pattern.finditer(text):
            key = (m.group(), bucket_name)
            if key not in seen:
                seen.add(key)
                results.append(MatchResult(term=m.group(), bucket=bucket_name))

    return results


# 正则组名 → 逻辑桶名映射
_REGEX_GROUP_BUCKET_MAP: dict[str, str] = {
    "date_patterns": "absolute_time",
    "time_patterns": "absolute_time",
    "weekday_patterns": "absolute_time",
    "range_patterns": "absolute_time",
    "duration_patterns": "duration",
}


def _regex_group_to_bucket(group: str) -> str:
    """将正则组名映射到逻辑桶名。"""
    return _REGEX_GROUP_BUCKET_MAP.get(group, group)
