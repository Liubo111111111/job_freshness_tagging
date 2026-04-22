from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class PromptBudgetResult:
    allowed: bool
    reason: str | None = None


def enforce_prompt_budget(node_name: str, payload: dict[str, Any]) -> PromptBudgetResult:
    if node_name == "final_decision" and "jobs_all_90d" in payload:
        return PromptBudgetResult(
            allowed=False,
            reason="raw_90d_payload_forbidden",
        )
    return PromptBudgetResult(allowed=True, reason=None)


def build_audit_record(**kwargs: Any) -> dict[str, Any]:
    return dict(kwargs)

