"""词表加载和管理 — 从 JSON 配置加载词表并编译正则模式。"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class Lexicon:
    """加载 recall_lexicon_v1.json，缓存编译后的词表和正则。"""

    def __init__(self, lexicon_path: str | Path) -> None:
        self._path = Path(lexicon_path)
        raw = self._load_json(self._path)

        # 词表分桶: bucket_name → list[str]
        self.term_buckets: dict[str, list[str]] = raw.get("term_buckets", {})

        # 正则模式分组: group_name → list[str]（原始字符串）
        self._raw_regex_patterns: dict[str, list[str]] = raw.get("regex_patterns", {})

        # 编译词表为正则（每个桶一个合并正则，按长度降序避免短词优先匹配）
        self.term_patterns: dict[str, re.Pattern[str]] = {}
        for bucket, terms in self.term_buckets.items():
            if terms:
                sorted_terms = sorted(terms, key=len, reverse=True)
                escaped = [re.escape(t) for t in sorted_terms]
                self.term_patterns[bucket] = re.compile("|".join(escaped))

        # 编译正则模式（每个组一个合并正则）
        self.regex_patterns: dict[str, re.Pattern[str]] = {}
        for group, patterns in self._raw_regex_patterns.items():
            if patterns:
                self.regex_patterns[group] = re.compile("|".join(patterns))

        logger.info(
            "词表加载完成: %d 个桶, %d 个正则组",
            len(self.term_buckets),
            len(self.regex_patterns),
        )

    @staticmethod
    def _load_json(path: Path) -> dict:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
