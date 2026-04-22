from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class LLMNormalizationResult:
    ok: bool
    data: dict[str, Any] | None
    error_type: str | None
    should_retry: bool


def _strip_code_fence(raw_text: str) -> str:
    text = raw_text.strip()
    if text.startswith("```") and text.endswith("```"):
        lines = text.splitlines()
        return "\n".join(lines[1:-1]).strip()
    return text


def normalize_llm_json(raw_text: str, schema_name: str, schema_model: type[Any] | None = None) -> LLMNormalizationResult:
    del schema_name
    candidate = _strip_code_fence(raw_text)
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        return LLMNormalizationResult(
            ok=False,
            data=None,
            error_type="parse_error",
            should_retry=False,
        )

    if schema_model is not None:
        try:
            validated = schema_model.model_validate(payload)
        except Exception:
            return LLMNormalizationResult(
                ok=False,
                data=None,
                error_type="schema_validation_error",
                should_retry=False,
            )
        return LLMNormalizationResult(
            ok=True,
            data=validated.model_dump(),
            error_type=None,
            should_retry=False,
        )

    if not isinstance(payload, dict):
        return LLMNormalizationResult(
            ok=False,
            data=None,
            error_type="schema_validation_error",
            should_retry=False,
        )

    return LLMNormalizationResult(
        ok=True,
        data=payload,
        error_type=None,
        should_retry=False,
    )

