from __future__ import annotations

from job_freshness.audit import build_audit_record
from job_freshness.graph_state import GraphState
from job_freshness.writers.jsonl_store import JsonlKeyedStore
from job_freshness.writers.sqlite_store import SqliteResultStore


def _publish_key(state: GraphState) -> str:
    return "::".join(
        [
            state.entity_key,
            state.feature_schema_version,
            state.graph_version,
        ]
    )


class FormalOutputWriter:
    """高置信结果写入器 — 双写 JSONL + SQLite。"""

    def __init__(
        self,
        jsonl_store: JsonlKeyedStore,
        sqlite_store: SqliteResultStore | None = None,
    ):
        self.jsonl_store = jsonl_store
        self.sqlite_store = sqlite_store

    def run(self, state: GraphState) -> GraphState:
        if state.decision_record is None:
            raise ValueError("formal_output_requires_decision_record")

        decision = state.decision_record
        publish_key = _publish_key(state)

        record: dict = {
            "info_id": state.entity_key,
            "validity_type": decision.validity_type,
            "estimated_expiry": decision.estimated_expiry,
            "reason": decision.reason,
            "audit": build_audit_record(
                run_id=state.run_id,
                entity_key=state.entity_key,
                graph_version=state.graph_version,
                feature_schema_version=state.feature_schema_version,
                prompt_version_detection=state.prompt_version_detection,
                prompt_version_normalization=state.prompt_version_normalization,
                prompt_version_risk=state.prompt_version_risk,
                prompt_version_final=state.prompt_version_final,
                model_version_detection=state.model_version_detection,
                model_version_normalization=state.model_version_normalization,
                model_version_risk=state.model_version_risk,
                model_version_final=state.model_version_final,
                route="formal",
            ),
        }

        # 写入 JSONL store
        self.jsonl_store[publish_key] = record

        # 写入 SQLite store
        if self.sqlite_store is not None:
            self.sqlite_store.upsert_run(state)
            self.sqlite_store.upsert_published_record(
                publish_key=publish_key,
                run_id=state.run_id,
                entity_key=state.entity_key,
                route="formal",
                record=record,
            )

        return state
