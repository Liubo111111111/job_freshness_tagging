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
            "fallback",
        ]
    )


class FallbackOutputWriter:
    """低置信/错误结果写入器 — 双写 JSONL + SQLite，保留已完成的中间结果。"""

    def __init__(
        self,
        jsonl_store: JsonlKeyedStore,
        sqlite_store: SqliteResultStore | None = None,
    ):
        self.jsonl_store = jsonl_store
        self.sqlite_store = sqlite_store

    def run(self, state: GraphState) -> GraphState:
        publish_key = _publish_key(state)

        # 保留已完成的中间结果
        decision = (
            state.decision_record.model_dump()
            if state.decision_record is not None
            else None
        )

        record: dict = {
            "info_id": state.entity_key,
            "error_type": state.error_type,
            "decision_record": decision,
            "audit": build_audit_record(
                run_id=state.run_id,
                entity_key=state.entity_key,
                error_type=state.error_type,
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
                route="fallback",
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
                route="fallback",
                record=record,
            )

        return state
