"""Service layer for the Freshness Pipeline Dashboard API.

Each service class reads freshness data from the SQLite store and exposes
thin business-logic methods that the route layer calls.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any

from job_freshness.api.schemas import (
    AccessSettingsResponse,
    AccessSettingsUpdate,
    AnnotationResponse,
    OnlineQueryResponse,
    PaginatedRunList,
    RunDetail,
    RunSummary,
    SearchResult,
    SettingsResponse,
    SettingsUpdate,
    StatsResponse,
)
from job_freshness.settings import (
    _load_env_values,
    load_llm_settings,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ENV_PATH = Path(__file__).resolve().parents[3] / ".env"


class _SqliteReader:
    """Thin SQLite reader for freshness pipeline data."""

    _INTERMEDIATE_COLS = ("snippet_recall_json", "signal_detection_json", "time_normalization_json")
    _RAW_WIDE_ROW_COL = "raw_wide_row_json"

    def __init__(self, sqlite_path: Path | None) -> None:
        self._path = sqlite_path
        self._has_intermediate_cols: bool | None = None
        self._has_raw_wide_row_col: bool = False

    @property
    def enabled(self) -> bool:
        if self._path is None or not self._path.exists():
            return False
        try:
            conn = sqlite3.connect(str(self._path))
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_runs'"
            ).fetchone()
            conn.close()
            return row is not None
        except Exception:
            return False

    def _connect(self) -> sqlite3.Connection:
        assert self._path is not None
        conn = sqlite3.connect(str(self._path))
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _load_json(payload: str | None, default: Any = None) -> Any:
        if not payload:
            return default
        try:
            return json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return default

    def _check_intermediate_cols(self, conn: sqlite3.Connection) -> bool:
        """检查 pipeline_runs 表是否包含中间记录列。"""
        if self._has_intermediate_cols is not None:
            return self._has_intermediate_cols
        existing = {
            row[1] for row in conn.execute("pragma table_info(pipeline_runs)").fetchall()
        }
        self._has_intermediate_cols = all(
            col in existing for col in self._INTERMEDIATE_COLS
        )
        self._has_raw_wide_row_col = self._RAW_WIDE_ROW_COL in existing
        return self._has_intermediate_cols


# ---------------------------------------------------------------------------
# StatsService
# ---------------------------------------------------------------------------


class StatsService:
    """Compute aggregate statistics for freshness pipeline results."""

    def __init__(self, sqlite_path: Path | None = None) -> None:
        self._sqlite = _SqliteReader(sqlite_path)

    def get_stats(self) -> StatsResponse:
        """返回 validity_type 分布统计。"""
        if not self._sqlite.enabled:
            return StatsResponse()

        validity_type_dist: dict[str, int] = {}
        total_count = 0
        formal_count = 0
        fallback_count = 0

        with self._sqlite._connect() as conn:
            # 按 route 统计
            rows = conn.execute(
                "SELECT route, COUNT(*) FROM pipeline_runs GROUP BY route"
            ).fetchall()
            for row in rows:
                route, cnt = row[0], row[1]
                total_count += cnt
                if route == "formal":
                    formal_count = cnt
                elif route == "fallback":
                    fallback_count = cnt

            # 从 decision_record_json 提取 validity_type 分布
            decision_rows = conn.execute(
                "SELECT decision_record_json FROM pipeline_runs WHERE decision_record_json IS NOT NULL"
            ).fetchall()
            for (dr_json,) in decision_rows:
                dr = self._sqlite._load_json(dr_json)
                if dr is None:
                    continue
                vt = dr.get("validity_type", "unknown")
                validity_type_dist[vt] = validity_type_dist.get(vt, 0) + 1

        return StatsResponse(
            validity_type_distribution=validity_type_dist,
            total_count=total_count,
            formal_count=formal_count,
            fallback_count=fallback_count,
        )


# ---------------------------------------------------------------------------
# RunService
# ---------------------------------------------------------------------------


class RunService:
    """List and detail freshness pipeline runs from SQLite."""

    def __init__(self, sqlite_path: Path | None = None) -> None:
        self._sqlite = _SqliteReader(sqlite_path)

    @staticmethod
    def _annotation_entry(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "run_id": row["run_id"],
            "entity_key": row["entity_key"],
            "annotated_label": row["annotated_label"],
            "reviewer_notes": row["reviewer_notes"],
            "reviewer_name": row["reviewer_name"],
            "created_at": row["created_at"],
        }

    def _annotation_map_by_entity(
        self, conn: sqlite3.Connection
    ) -> dict[str, list[dict[str, Any]]]:
        rows = conn.execute(
            """
            SELECT run_id, entity_key, annotated_label, reviewer_notes, reviewer_name, created_at
            FROM annotations
            ORDER BY created_at ASC, id ASC
            """
        ).fetchall()
        result: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            result.setdefault(row["entity_key"], []).append(self._annotation_entry(row))
        return result

    def list_runs(
        self,
        offset: int = 0,
        limit: int = 20,
        annotation_status: str | None = None,
    ) -> PaginatedRunList:
        """返回分页的运行记录列表，包含新鲜度字段。"""
        if not self._sqlite.enabled:
            return PaginatedRunList(items=[], total=0, offset=offset, limit=limit)

        with self._sqlite._connect() as conn:
            annotations_by_entity = self._annotation_map_by_entity(conn)
            rows = conn.execute(
                """
                WITH latest AS (
                    SELECT entity_key, MAX(updated_at) AS max_updated
                    FROM pipeline_runs
                    GROUP BY entity_key
                )
                SELECT
                    r.run_id,
                    r.entity_key,
                    r.route,
                    r.error_type,
                    r.decision_record_json,
                    r.risk_record_json,
                    r.created_at
                FROM pipeline_runs r
                INNER JOIN latest
                    ON r.entity_key = latest.entity_key
                   AND r.updated_at = latest.max_updated
                ORDER BY r.updated_at DESC, r.run_id DESC
                """,
            ).fetchall()

        all_items: list[RunSummary] = []
        for row in rows:
            decision = self._sqlite._load_json(row["decision_record_json"])
            risk = self._sqlite._load_json(row["risk_record_json"])
            annotations = annotations_by_entity.get(row["entity_key"], [])
            latest_annotated = (
                annotations[-1]["annotated_label"] if annotations else None
            )

            validity_type: str | None = None
            estimated_expiry: str | None = None
            stale_risk_hint: bool | None = None
            complaint_risk_hint: Any = None

            if decision:
                validity_type = decision.get("validity_type")
                estimated_expiry = decision.get("estimated_expiry")
                stale_risk_hint = decision.get("stale_risk_hint")
                crh = decision.get("complaint_risk_hint")
                if crh:
                    complaint_risk_hint = crh
            elif risk:
                stale_risk_hint = risk.get("stale_risk_hint")
                complaint_risk_hint = risk.get("complaint_risk_hint")

            all_items.append(
                RunSummary(
                    run_id=row["run_id"],
                    entity_key=row["entity_key"],
                    validity_type=validity_type,
                    estimated_expiry=estimated_expiry,
                    stale_risk_hint=stale_risk_hint,
                    complaint_risk_hint=complaint_risk_hint,
                    route=row["route"],
                    error_type=row["error_type"],
                    timestamp=row["created_at"],
                    annotated_label=latest_annotated,
                    annotations=annotations,
                )
            )

        filtered_items = all_items
        if annotation_status == "annotated":
            filtered_items = [item for item in all_items if item.annotations]
        elif annotation_status == "unannotated":
            filtered_items = [item for item in all_items if not item.annotations]

        total = len(filtered_items)
        items = filtered_items[offset : offset + limit]
        return PaginatedRunList(items=items, total=total, offset=offset, limit=limit)

    def get_run_detail(self, run_id: str) -> RunDetail | None:
        """返回单条运行记录的完整详情，包含 step-level audit。"""
        if not self._sqlite.enabled:
            return None

        with self._sqlite._connect() as conn:
            has_intermediate = self._sqlite._check_intermediate_cols(conn)
            intermediate_select = (
                "snippet_recall_json, signal_detection_json, time_normalization_json,"
                if has_intermediate
                else ""
            )
            raw_wide_row_select = (
                "raw_wide_row_json,"
                if self._sqlite._has_raw_wide_row_col
                else ""
            )
            row = conn.execute(
                f"""
                SELECT
                    run_id,
                    entity_key,
                    route,
                    error_type,
                    wide_row_json,
                    {raw_wide_row_select}
                    {intermediate_select}
                    temporal_signal_json,
                    risk_record_json,
                    decision_record_json,
                    timing_ms_json,
                    feature_schema_version,
                    graph_version,
                    prompt_version_detection,
                    prompt_version_normalization,
                    prompt_version_risk,
                    prompt_version_final,
                    model_version_detection,
                    model_version_normalization,
                    model_version_risk,
                    model_version_final
                FROM pipeline_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()

            if row is None:
                return None

            # 获取 step-level audit
            steps = conn.execute(
                """
                SELECT step_name, prompt_version, model_version, payload_json, result_json, error_type, created_at
                FROM inference_steps
                WHERE run_id = ?
                ORDER BY created_at ASC
                """,
                (run_id,),
            ).fetchall()
            annotation_rows = conn.execute(
                """
                SELECT run_id, entity_key, annotated_label, reviewer_notes, reviewer_name, created_at
                FROM annotations
                WHERE entity_key = (
                    SELECT entity_key
                    FROM pipeline_runs
                    WHERE run_id = ?
                )
                ORDER BY created_at ASC, id ASC
                """,
                (run_id,),
            ).fetchall()

        wide_row = self._sqlite._load_json(row["wide_row_json"], {})
        raw_wide_row = self._sqlite._load_json(row["raw_wide_row_json"]) if self._sqlite._has_raw_wide_row_col else None
        snippet_recall = self._sqlite._load_json(row["snippet_recall_json"]) if has_intermediate else None
        signal_detection = self._sqlite._load_json(row["signal_detection_json"]) if has_intermediate else None
        time_normalization = self._sqlite._load_json(row["time_normalization_json"]) if has_intermediate else None
        temporal_signal = self._sqlite._load_json(row["temporal_signal_json"])
        risk_record = self._sqlite._load_json(row["risk_record_json"])
        decision_record = self._sqlite._load_json(row["decision_record_json"])
        timing_ms = self._sqlite._load_json(row["timing_ms_json"])
        annotations = [self._annotation_entry(annotation_row) for annotation_row in annotation_rows]

        # 构建 audit 信息
        audit: dict[str, Any] = {
            "run_id": row["run_id"],
            "entity_key": row["entity_key"],
            "feature_schema_version": row["feature_schema_version"],
            "graph_version": row["graph_version"],
            "prompt_version_detection": row["prompt_version_detection"],
            "prompt_version_normalization": row["prompt_version_normalization"],
            "prompt_version_risk": row["prompt_version_risk"],
            "prompt_version_final": row["prompt_version_final"],
            "model_version_detection": row["model_version_detection"],
            "model_version_normalization": row["model_version_normalization"],
            "model_version_risk": row["model_version_risk"],
            "model_version_final": row["model_version_final"],
            "route": row["route"],
            "steps": [],
        }

        for step in steps:
            audit["steps"].append(
                {
                    "step_name": step["step_name"],
                    "prompt_version": step["prompt_version"],
                    "model_version": step["model_version"],
                    "payload": self._sqlite._load_json(step["payload_json"], {}),
                    "result": self._sqlite._load_json(step["result_json"], {}),
                    "error_type": step["error_type"],
                    "created_at": step["created_at"],
                }
            )

        return RunDetail(
            run_id=row["run_id"],
            entity_key=row["entity_key"],
            wide_row=wide_row,
            raw_wide_row=raw_wide_row,
            snippet_recall_record=snippet_recall,
            signal_detection_record=signal_detection,
            time_normalization_record=time_normalization,
            temporal_signal_record=temporal_signal,
            risk_record=risk_record,
            decision_record=decision_record,
            route=row["route"],
            error_type=row["error_type"],
            audit=audit,
            timing_ms=timing_ms,
            annotations=annotations,
        )

    def annotate(
        self,
        run_id: str,
        annotated_label: str,
        reviewer_notes: str = "",
        reviewer_name: str = "",
    ) -> AnnotationResponse | None:
        """为指定运行记录追加人工标注。"""
        if not self._sqlite.enabled:
            return None

        with self._sqlite._connect() as conn:
            run_row = conn.execute(
                "SELECT entity_key FROM pipeline_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run_row is None:
                return None

            annotation_count = conn.execute(
                "SELECT COUNT(*) FROM annotations WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]
            if annotation_count >= 3:
                return AnnotationResponse(
                    run_id=run_id,
                    annotated_label=annotated_label,
                    status="max_reached",
                    annotation_count=annotation_count,
                )

            conn.execute(
                """
                INSERT INTO annotations (
                    run_id,
                    entity_key,
                    annotated_label,
                    reviewer_notes,
                    reviewer_name
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    run_row["entity_key"],
                    annotated_label,
                    reviewer_notes,
                    reviewer_name,
                ),
            )
            conn.commit()

            new_count = conn.execute(
                "SELECT COUNT(*) FROM annotations WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]

        return AnnotationResponse(
            run_id=run_id,
            annotated_label=annotated_label,
            status="annotated",
            annotation_count=new_count,
        )


# ---------------------------------------------------------------------------
# SearchService
# ---------------------------------------------------------------------------


class SearchService:
    """Search pipeline runs by info_id (entity_key)."""

    def __init__(self, sqlite_path: Path | None = None) -> None:
        self._sqlite = _SqliteReader(sqlite_path)

    def search(self, query: str) -> list[SearchResult]:
        """按 info_id 搜索运行记录。"""
        if not query or not self._sqlite.enabled:
            return []

        with self._sqlite._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    r.entity_key,
                    r.route,
                    r.run_id,
                    r.decision_record_json
                FROM pipeline_runs r
                INNER JOIN (
                    SELECT entity_key, MAX(updated_at) AS max_updated
                    FROM pipeline_runs
                    GROUP BY entity_key
                ) latest
                    ON r.entity_key = latest.entity_key
                   AND r.updated_at = latest.max_updated
                WHERE r.entity_key LIKE ?
                ORDER BY r.updated_at DESC
                LIMIT 50
                """,
                (f"%{query}%",),
            ).fetchall()

        results: list[SearchResult] = []
        for row in rows:
            decision = self._sqlite._load_json(row["decision_record_json"])
            validity_type: str | None = None
            estimated_expiry: str | None = None
            if decision:
                validity_type = decision.get("validity_type")
                estimated_expiry = decision.get("estimated_expiry")

            results.append(
                SearchResult(
                    entity_key=row["entity_key"],
                    validity_type=validity_type,
                    estimated_expiry=estimated_expiry,
                    route=row["route"],
                    run_id=row["run_id"],
                )
            )

        return results


# ---------------------------------------------------------------------------
# OnlineQueryService
# ---------------------------------------------------------------------------


class OnlineQueryService:
    """按 info_id 列表实时拉取 ODPS 宽表并执行流水线。"""

    def __init__(self, partition_dir: Path) -> None:
        self._partition_dir = partition_dir
        self._partition_dir.mkdir(parents=True, exist_ok=True)
        self._sqlite_path = self._partition_dir / "pipeline_results.sqlite3"
        self._run_service = RunService(self._sqlite_path)

    def query(self, info_ids: list[str], pt: str) -> OnlineQueryResponse:
        """对指定 info_ids 执行实时查询，未命中的返回 not_found。"""
        normalized_ids: list[str] = []
        for info_id in info_ids:
            normalized = info_id.strip()
            if normalized and normalized not in normalized_ids:
                normalized_ids.append(normalized)

        if not normalized_ids:
            return OnlineQueryResponse(results=[], not_found=[])

        from uuid import uuid4

        from job_freshness.data_fetcher import fetch_freshness_candidates_by_info_ids
        from job_freshness.llm.client import HttpLLMClient
        from job_freshness.loader import load_wide_rows
        from job_freshness.main import run_once
        from job_freshness.writers.fallback_output import FallbackOutputWriter
        from job_freshness.writers.formal_output import FormalOutputWriter
        from job_freshness.writers.jsonl_store import JsonlKeyedStore
        from job_freshness.writers.sqlite_store import SqliteResultStore

        raw_rows = fetch_freshness_candidates_by_info_ids(pt, normalized_ids)
        load_result = load_wide_rows(raw_rows)
        wide_rows_by_info_id = {row.info_id: row for row in load_result.rows}

        results: list[RunDetail] = []
        formal_store = JsonlKeyedStore(self._partition_dir / "formal_output.jsonl")
        fallback_store = JsonlKeyedStore(self._partition_dir / "fallback_output.jsonl")
        sqlite_store = SqliteResultStore(self._sqlite_path)
        llm_client: Any | None = None

        try:
            llm_client = HttpLLMClient()
            formal_writer = FormalOutputWriter(
                jsonl_store=formal_store,
                sqlite_store=sqlite_store,
            )
            fallback_writer = FallbackOutputWriter(
                jsonl_store=fallback_store,
                sqlite_store=sqlite_store,
            )
            for info_id in normalized_ids:
                wide_row = wide_rows_by_info_id.get(info_id)
                if wide_row is None:
                    continue

                # 在线查询重新执行时，旧记录按 entity_key 全量覆盖。
                formal_store.delete_by_prefix(f"{info_id}::")
                fallback_store.delete_by_prefix(f"{info_id}::")
                sqlite_store.delete_entity(info_id)

                state = run_once(
                    wide_row=wide_row,
                    run_id=f"online-{uuid4().hex}",
                    client=llm_client,
                    formal_writer=formal_writer,
                    fallback_writer=fallback_writer,
                )
                detail = self._run_service.get_run_detail(state.run_id)
                if detail is not None:
                    results.append(detail)
        finally:
            sqlite_store.close()
            if llm_client is not None:
                llm_client.close()

        found_ids = {detail.entity_key for detail in results}
        not_found = [info_id for info_id in normalized_ids if info_id not in found_ids]
        return OnlineQueryResponse(results=results, not_found=not_found)


# ---------------------------------------------------------------------------
# SettingsService
# ---------------------------------------------------------------------------

# Mapping from SettingsUpdate field names → .env variable names
_SETTINGS_ENV_MAP: dict[str, str] = {
    "llm_model": "LLM_MODEL",
    "llm_timeout_sec": "LLM_TIMEOUT_SEC",
    "llm_max_retry": "LLM_MAX_RETRY",
    "worker_count": "WORKER_COUNT",
    "provider_rate_limit_per_minute": "PROVIDER_RATE_LIMIT_PER_MINUTE",
    "max_in_flight": "MAX_IN_FLIGHT",
    "batch_max_rows": "BATCH_MAX_ROWS",
    "fetch_only_filled_complaints": "FETCH_ONLY_FILLED_COMPLAINTS",
}

_RUNTIME_DEFAULTS: dict[str, int] = {
    "worker_count": 4,
    "provider_rate_limit_per_minute": 120,
    "max_in_flight": 8,
    "batch_max_rows": 5,
}

_ACCESS_SETTINGS_ENV_MAP: dict[str, str] = {
    "allowed_open_ids": "FEISHU_ALLOWED_OPEN_IDS",
    "allowed_emails": "FEISHU_ALLOWED_EMAILS",
    "admin_open_ids": "FEISHU_ADMIN_OPEN_IDS",
    "admin_emails": "FEISHU_ADMIN_EMAILS",
}


class SettingsService:
    """Read / write runtime configuration stored in the ``.env`` file."""

    def __init__(self, env_path: Path | None = None) -> None:
        self._env_path = env_path or _ENV_PATH
        self._uses_default_path = env_path is None

    def _read_env(self) -> dict[str, str]:
        if self._uses_default_path:
            return dict(_load_env_values())
        from dotenv import dotenv_values
        return {k: v for k, v in dotenv_values(self._env_path).items() if v is not None}

    def get_settings(self) -> SettingsResponse:
        env = self._read_env()
        return SettingsResponse(
            llm_model=env.get("LLM_MODEL", "qwen3-max"),
            llm_timeout_sec=int(env.get("LLM_TIMEOUT_SEC", "30")),
            llm_max_retry=int(env.get("LLM_MAX_RETRY", "2")),
            worker_count=int(env.get("WORKER_COUNT", str(_RUNTIME_DEFAULTS["worker_count"]))),
            provider_rate_limit_per_minute=int(
                env.get("PROVIDER_RATE_LIMIT_PER_MINUTE", str(_RUNTIME_DEFAULTS["provider_rate_limit_per_minute"]))
            ),
            max_in_flight=int(env.get("MAX_IN_FLIGHT", str(_RUNTIME_DEFAULTS["max_in_flight"]))),
            batch_max_rows=int(env.get("BATCH_MAX_ROWS", str(_RUNTIME_DEFAULTS["batch_max_rows"]))),
            fetch_only_filled_complaints=self._parse_bool(env.get("FETCH_ONLY_FILLED_COMPLAINTS", "false")),
        )

    def get_access_settings(self) -> AccessSettingsResponse:
        env = self._read_env()
        return AccessSettingsResponse(
            allowed_open_ids=self._parse_csv_list(env.get("FEISHU_ALLOWED_OPEN_IDS", "")),
            allowed_emails=self._parse_csv_list(env.get("FEISHU_ALLOWED_EMAILS", ""), lowercase=True),
            admin_open_ids=self._parse_csv_list(env.get("FEISHU_ADMIN_OPEN_IDS", "")),
            admin_emails=self._parse_csv_list(env.get("FEISHU_ADMIN_EMAILS", ""), lowercase=True),
        )

    def update_settings(self, update: SettingsUpdate) -> SettingsResponse:
        changes: dict[str, str] = {}
        for field_name, env_var in _SETTINGS_ENV_MAP.items():
            value = getattr(update, field_name, None)
            if value is not None:
                changes[env_var] = str(value).lower() if isinstance(value, bool) else str(value)

        if changes:
            self._patch_env_file(changes)
            for env_var, val in changes.items():
                os.environ[env_var] = val
            _load_env_values.cache_clear()
            load_llm_settings.cache_clear()

        return self.get_settings()

    def update_access_settings(self, update: AccessSettingsUpdate) -> AccessSettingsResponse:
        normalized = {
            "allowed_open_ids": self._normalize_items(update.allowed_open_ids),
            "allowed_emails": self._normalize_items(update.allowed_emails, lowercase=True),
            "admin_open_ids": self._normalize_items(update.admin_open_ids),
            "admin_emails": self._normalize_items(update.admin_emails, lowercase=True),
        }
        changes = {
            env_var: ",".join(normalized[field_name])
            for field_name, env_var in _ACCESS_SETTINGS_ENV_MAP.items()
        }
        self._patch_env_file(changes)
        _load_env_values.cache_clear()
        load_llm_settings.cache_clear()
        return AccessSettingsResponse(**normalized)

    def _patch_env_file(self, changes: dict[str, str]) -> None:
        if self._env_path.exists():
            lines = self._env_path.read_text(encoding="utf-8").splitlines()
        else:
            lines = []

        updated_keys: set[str] = set()
        new_lines: list[str] = []

        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                new_lines.append(line)
                continue
            key = stripped.split("=", 1)[0].strip()
            if key in changes:
                new_lines.append(f"{key}={changes[key]}")
                updated_keys.add(key)
            else:
                new_lines.append(line)

        for key, val in changes.items():
            if key not in updated_keys:
                new_lines.append(f"{key}={val}")

        self._env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

    @staticmethod
    def _parse_csv_list(raw: str, lowercase: bool = False) -> list[str]:
        items = [item.strip() for item in raw.split(",") if item.strip()]
        if lowercase:
            return [item.lower() for item in items]
        return items

    @staticmethod
    def _normalize_items(items: list[str], lowercase: bool = False) -> list[str]:
        normalized: list[str] = []
        for item in items:
            value = item.strip()
            if not value:
                continue
            value = value.lower() if lowercase else value
            if value not in normalized:
                normalized.append(value)
        return normalized

    @staticmethod
    def _parse_bool(raw: str) -> bool:
        return raw.strip().lower() in {"1", "true", "yes", "on"}
