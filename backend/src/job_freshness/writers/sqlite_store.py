from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from job_freshness.graph_state import GraphState


class SqliteResultStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                create table if not exists pipeline_runs (
                    run_id text primary key,
                    entity_key text not null,
                    route text not null,
                    error_type text,
                    feature_schema_version text not null,
                    graph_version text not null,
                    prompt_version_detection text not null,
                    prompt_version_normalization text not null,
                    prompt_version_risk text not null,
                    prompt_version_final text not null,
                    model_version_detection text not null,
                    model_version_normalization text not null,
                    model_version_risk text not null,
                    model_version_final text not null,
                    wide_row_json text not null,
                    raw_wide_row_json text,
                    snippet_recall_json text,
                    signal_detection_json text,
                    time_normalization_json text,
                    temporal_signal_json text,
                    risk_record_json text,
                    decision_record_json text,
                    timing_ms_json text,
                    created_at text not null default current_timestamp,
                    updated_at text not null default current_timestamp
                );

                create table if not exists inference_steps (
                    run_id text not null,
                    step_name text not null,
                    entity_key text not null,
                    prompt_version text not null,
                    model_version text not null,
                    payload_json text not null,
                    result_json text not null,
                    error_type text,
                    created_at text not null default current_timestamp,
                    updated_at text not null default current_timestamp,
                    primary key (run_id, step_name)
                );

                create table if not exists published_records (
                    publish_key text primary key,
                    run_id text not null,
                    entity_key text not null,
                    route text not null,
                    record_json text not null,
                    created_at text not null default current_timestamp,
                    updated_at text not null default current_timestamp
                );

                create index if not exists idx_inference_steps_run_id
                on inference_steps (run_id);

                create index if not exists idx_published_records_run_id
                on published_records (run_id);

                create table if not exists annotations (
                    id integer primary key autoincrement,
                    run_id text not null,
                    entity_key text not null,
                    annotated_label text not null,
                    reviewer_notes text not null default '',
                    reviewer_name text not null default '',
                    created_at text not null default current_timestamp
                );

                create index if not exists idx_annotations_run_id
                on annotations (run_id);
                """
            )
            self._conn.commit()
            # 迁移：为已有数据库添加中间记录列
            self._migrate_add_intermediate_columns()

    def _migrate_add_intermediate_columns(self) -> None:
        """为已有数据库添加 snippet_recall_json / signal_detection_json / time_normalization_json 列。"""
        existing = {
            row[1]
            for row in self._conn.execute("pragma table_info(pipeline_runs)").fetchall()
        }
        migrations = [
            ("snippet_recall_json", "text"),
            ("signal_detection_json", "text"),
            ("time_normalization_json", "text"),
            ("raw_wide_row_json", "text"),
        ]
        for col_name, col_type in migrations:
            if col_name not in existing:
                self._conn.execute(
                    f"alter table pipeline_runs add column {col_name} {col_type}"
                )
        self._conn.commit()

    @staticmethod
    def _dump_json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    def upsert_run(self, state: GraphState) -> None:
        with self._lock:
            self._conn.execute(
                """
                insert into pipeline_runs (
                    run_id,
                    entity_key,
                    route,
                    error_type,
                    feature_schema_version,
                    graph_version,
                    prompt_version_detection,
                    prompt_version_normalization,
                    prompt_version_risk,
                    prompt_version_final,
                    model_version_detection,
                    model_version_normalization,
                    model_version_risk,
                    model_version_final,
                    wide_row_json,
                    raw_wide_row_json,
                    snippet_recall_json,
                    signal_detection_json,
                    time_normalization_json,
                    temporal_signal_json,
                    risk_record_json,
                    decision_record_json,
                    timing_ms_json,
                    updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                on conflict(run_id) do update set
                    entity_key = excluded.entity_key,
                    route = excluded.route,
                    error_type = excluded.error_type,
                    feature_schema_version = excluded.feature_schema_version,
                    graph_version = excluded.graph_version,
                    prompt_version_detection = excluded.prompt_version_detection,
                    prompt_version_normalization = excluded.prompt_version_normalization,
                    prompt_version_risk = excluded.prompt_version_risk,
                    prompt_version_final = excluded.prompt_version_final,
                    model_version_detection = excluded.model_version_detection,
                    model_version_normalization = excluded.model_version_normalization,
                    model_version_risk = excluded.model_version_risk,
                    model_version_final = excluded.model_version_final,
                    wide_row_json = excluded.wide_row_json,
                    raw_wide_row_json = excluded.raw_wide_row_json,
                    snippet_recall_json = excluded.snippet_recall_json,
                    signal_detection_json = excluded.signal_detection_json,
                    time_normalization_json = excluded.time_normalization_json,
                    temporal_signal_json = excluded.temporal_signal_json,
                    risk_record_json = excluded.risk_record_json,
                    decision_record_json = excluded.decision_record_json,
                    timing_ms_json = excluded.timing_ms_json,
                    updated_at = current_timestamp
                """,
                (
                    state.run_id,
                    state.entity_key,
                    state.route,
                    state.error_type,
                    state.feature_schema_version,
                    state.graph_version,
                    state.prompt_version_detection,
                    state.prompt_version_normalization,
                    state.prompt_version_risk,
                    state.prompt_version_final,
                    state.model_version_detection,
                    state.model_version_normalization,
                    state.model_version_risk,
                    state.model_version_final,
                    self._dump_json(state.wide_row.model_dump()),
                    self._dump_json(state.raw_wide_row.model_dump()) if state.raw_wide_row else None,
                    self._dump_json(state.snippet_recall_record.model_dump()) if state.snippet_recall_record else None,
                    self._dump_json(state.signal_detection_record.model_dump()) if state.signal_detection_record else None,
                    self._dump_json(state.time_normalization_record.model_dump()) if state.time_normalization_record else None,
                    self._dump_json(state.temporal_signal_record.model_dump()) if state.temporal_signal_record else None,
                    self._dump_json(state.risk_record.model_dump()) if state.risk_record else None,
                    self._dump_json(state.decision_record.model_dump()) if state.decision_record else None,
                    self._dump_json(state.timing_ms),
                ),
            )
            self._conn.commit()

    def upsert_step(
        self,
        run_id: str,
        entity_key: str,
        step_name: str,
        prompt_version: str,
        model_version: str,
        payload: dict[str, Any],
        result: dict[str, Any],
        error_type: str | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                insert into inference_steps (
                    run_id,
                    step_name,
                    entity_key,
                    prompt_version,
                    model_version,
                    payload_json,
                    result_json,
                    error_type,
                    updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                on conflict(run_id, step_name) do update set
                    entity_key = excluded.entity_key,
                    prompt_version = excluded.prompt_version,
                    model_version = excluded.model_version,
                    payload_json = excluded.payload_json,
                    result_json = excluded.result_json,
                    error_type = excluded.error_type,
                    updated_at = current_timestamp
                """,
                (
                    run_id,
                    step_name,
                    entity_key,
                    prompt_version,
                    model_version,
                    self._dump_json(payload),
                    self._dump_json(result),
                    error_type,
                ),
            )
            self._conn.commit()

    def upsert_published_record(
        self,
        publish_key: str,
        run_id: str,
        entity_key: str,
        route: str,
        record: dict[str, Any],
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                insert into published_records (
                    publish_key,
                    run_id,
                    entity_key,
                    route,
                    record_json,
                    updated_at
                ) values (?, ?, ?, ?, ?, current_timestamp)
                on conflict(publish_key) do update set
                    run_id = excluded.run_id,
                    entity_key = excluded.entity_key,
                    route = excluded.route,
                    record_json = excluded.record_json,
                    updated_at = current_timestamp
                """,
                (
                    publish_key,
                    run_id,
                    entity_key,
                    route,
                    self._dump_json(record),
                ),
            )
            self._conn.commit()

    def add_annotation(
        self,
        run_id: str,
        entity_key: str,
        annotated_label: str,
        reviewer_notes: str,
        reviewer_name: str = "",
    ) -> None:
        with self._lock:
            count = self._conn.execute(
                "select count(*) from annotations where run_id = ?",
                (run_id,),
            ).fetchone()[0]
            if count >= 3:
                raise ValueError("max_3_annotations_per_run")
            self._conn.execute(
                """
                insert into annotations (
                    run_id,
                    entity_key,
                    annotated_label,
                    reviewer_notes,
                    reviewer_name
                ) values (?, ?, ?, ?, ?)
                """,
                (run_id, entity_key, annotated_label, reviewer_notes, reviewer_name),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
