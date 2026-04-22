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
        """返回 temporal_status 分布 + signal_type 分布。"""
        if not self._sqlite.enabled:
            return StatsResponse()

        temporal_status_dist: dict[str, int] = {}
        signal_type_dist: dict[str, int] = {}
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

            # 从 decision_record_json 提取 temporal_status 和 signal_type 分布
            decision_rows = conn.execute(
                "SELECT decision_record_json FROM pipeline_runs WHERE decision_record_json IS NOT NULL"
            ).fetchall()
            for (dr_json,) in decision_rows:
                dr = self._sqlite._load_json(dr_json)
                if dr is None:
                    continue
                ts = dr.get("temporal_status", "unknown")
                temporal_status_dist[ts] = temporal_status_dist.get(ts, 0) + 1
                st = dr.get("signal_type", "unknown")
                signal_type_dist[st] = signal_type_dist.get(st, 0) + 1

        return StatsResponse(
            temporal_status_distribution=temporal_status_dist,
            signal_type_distribution=signal_type_dist,
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

    def list_runs(
        self,
        offset: int = 0,
        limit: int = 20,
    ) -> PaginatedRunList:
        """返回分页的运行记录列表，包含新鲜度字段。"""
        if not self._sqlite.enabled:
            return PaginatedRunList(items=[], total=0, offset=offset, limit=limit)

        with self._sqlite._connect() as conn:
            # 总数
            total_row = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()
            total = total_row[0] if total_row else 0

            # 分页查询（按 entity_key 去重，取最新）
            rows = conn.execute(
                """
                SELECT
                    r.run_id,
                    r.entity_key,
                    r.route,
                    r.error_type,
                    r.decision_record_json,
                    r.temporal_signal_json,
                    r.risk_record_json,
                    r.created_at
                FROM pipeline_runs r
                INNER JOIN (
                    SELECT entity_key, MAX(updated_at) AS max_updated
                    FROM pipeline_runs
                    GROUP BY entity_key
                ) latest
                    ON r.entity_key = latest.entity_key
                   AND r.updated_at = latest.max_updated
                ORDER BY r.updated_at DESC, r.run_id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()

        items: list[RunSummary] = []
        for row in rows:
            decision = self._sqlite._load_json(row["decision_record_json"])
            risk = self._sqlite._load_json(row["risk_record_json"])

            temporal_status: str | None = None
            signal_type: str | None = None
            confidence: float | None = None
            stale_risk_hint: bool | None = None
            complaint_risk_hint: Any = None

            if decision:
                temporal_status = decision.get("temporal_status")
                signal_type = decision.get("signal_type")
                confidence = decision.get("confidence")
                stale_risk_hint = decision.get("stale_risk_hint")
                crh = decision.get("complaint_risk_hint")
                if crh:
                    complaint_risk_hint = crh
            elif risk:
                stale_risk_hint = risk.get("stale_risk_hint")
                complaint_risk_hint = risk.get("complaint_risk_hint")

            items.append(
                RunSummary(
                    run_id=row["run_id"],
                    entity_key=row["entity_key"],
                    temporal_status=temporal_status,
                    signal_type=signal_type,
                    confidence=confidence,
                    stale_risk_hint=stale_risk_hint,
                    complaint_risk_hint=complaint_risk_hint,
                    route=row["route"],
                    error_type=row["error_type"],
                    timestamp=row["created_at"],
                )
            )

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

        wide_row = self._sqlite._load_json(row["wide_row_json"], {})
        raw_wide_row = self._sqlite._load_json(row["raw_wide_row_json"]) if self._sqlite._has_raw_wide_row_col else None
        snippet_recall = self._sqlite._load_json(row["snippet_recall_json"]) if has_intermediate else None
        signal_detection = self._sqlite._load_json(row["signal_detection_json"]) if has_intermediate else None
        time_normalization = self._sqlite._load_json(row["time_normalization_json"]) if has_intermediate else None
        temporal_signal = self._sqlite._load_json(row["temporal_signal_json"])
        risk_record = self._sqlite._load_json(row["risk_record_json"])
        decision_record = self._sqlite._load_json(row["decision_record_json"])
        timing_ms = self._sqlite._load_json(row["timing_ms_json"])

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
            temporal_status: str | None = None
            signal_type: str | None = None
            if decision:
                temporal_status = decision.get("temporal_status")
                signal_type = decision.get("signal_type")

            results.append(
                SearchResult(
                    entity_key=row["entity_key"],
                    temporal_status=temporal_status,
                    signal_type=signal_type,
                    route=row["route"],
                    run_id=row["run_id"],
                )
            )

        return results


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
                changes[env_var] = str(value)

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
