from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field

_env_path = Path(__file__).resolve().parents[2] / ".env"


def _env_candidates() -> list[Path]:
    project_root = _env_path.parent
    repo_root = project_root.parent
    candidates = [_env_path]
    fallback_env = repo_root / ".env"
    if fallback_env != _env_path:
        candidates.append(fallback_env)
    return candidates


@lru_cache(maxsize=1)
def _load_env_values() -> dict[str, str]:
    for env_path in _env_candidates():
        if env_path.exists():
            return {
                key: value
                for key, value in dotenv_values(env_path).items()
                if value is not None
            }
    return {}


def _env_get(name: str, default: str = "") -> str:
    return _load_env_values().get(name, default)


# ---------------------------------------------------------------------------
# LLM 配置
# ---------------------------------------------------------------------------

class LLMSettings(BaseModel):
    """从环境变量读取的 LLM 配置。"""
    model_config = ConfigDict(extra="forbid")

    api_key: str = ""
    base_url: str = ""
    model: str = "qwen3-max"
    timeout_sec: int = 30
    max_retry: int = 2


@lru_cache(maxsize=1)
def load_llm_settings() -> LLMSettings:
    api_key = (
        _env_get("DASHSCOPE_API_KEY")
        or _env_get("OPENAI_API_KEY")
        or _env_get("LLM_API_KEY")
        or ""
    )
    base_url = (
        _env_get("LLM_BASE_URL")
        or _env_get("DASHSCOPE_BASE_URL")
        or _env_get("OPENAI_BASE_URL")
        or ""
    )
    return LLMSettings(
        api_key=api_key,
        base_url=base_url,
        model=_env_get("LLM_MODEL", "qwen3-max"),
        timeout_sec=int(_env_get("LLM_TIMEOUT_SEC", "30")),
        max_retry=int(_env_get("LLM_MAX_RETRY", "2")),
    )


# ---------------------------------------------------------------------------
# ODPS 配置
# ---------------------------------------------------------------------------

class ODPSSettings(BaseModel):
    """从环境变量读取的 ODPS (MaxCompute) 配置。"""
    model_config = ConfigDict(extra="forbid")

    access_key_id: str = ""
    access_key_secret: str = ""
    project: str = ""
    endpoint: str = ""


@lru_cache(maxsize=1)
def load_odps_settings() -> ODPSSettings:
    return ODPSSettings(
        access_key_id=_env_get("ODPS_ACCESS_KEY_ID", ""),
        access_key_secret=_env_get("ODPS_ACCESS_KEY_SECRET", ""),
        project=_env_get("ODPS_PROJECT", ""),
        endpoint=_env_get("ODPS_ENDPOINT", ""),
    )


class PromptAsset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    system_prompt: str
    user_template: str


def _prompt_asset_path(node_name: str, version: str) -> Path:
    return Path(__file__).with_name("prompts") / f"{node_name}_{version}.yaml"


@lru_cache(maxsize=16)
def load_prompt_asset(node_name: str, version: str) -> PromptAsset:
    with _prompt_asset_path(node_name, version).open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)
    return PromptAsset.model_validate(payload)
