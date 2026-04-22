"""缓存键构建工具。"""
from __future__ import annotations

import hashlib


def build_cache_key(
    entity_key: str,
    input_hash: str,
    graph_version: str,
    prompt_version: str,
    model_version: str,
) -> str:
    """构建缓存键：基于实体、输入哈希和版本维度。"""
    raw = "::".join(
        [
            entity_key,
            input_hash,
            graph_version,
            prompt_version,
            model_version,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
