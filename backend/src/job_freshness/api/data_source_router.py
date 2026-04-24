"""DataSourceRouter – route API requests to date-partitioned data directories.

Each batch run produces output under ``output/{pt}/`` where *pt* is a
``yyyymmdd`` date string.  Legacy data lives under ``output/_legacy/``.
This module validates *pt* values, resolves directory paths, and builds
per-partition service instances on the fly.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from job_freshness.api.schemas import StatsResponse
from job_freshness.api.services import (
    OnlineQueryService,
    RunService,
    SearchService,
    StatsService,
)

logger = logging.getLogger(__name__)

_YYYYMMDD_RE = re.compile(r"^\d{8}$")
_DATE_RANGE_RE = re.compile(r"^(\d{8})_(\d{8})$")


@dataclass
class _DatesCache:
    """In-memory cache for the date list returned by ``list_dates()``."""

    entries: list[dict] = field(default_factory=list)
    latest_pt: str | None = None
    expires_at: float = 0.0


class DataSourceRouter:
    """Map a ``pt`` parameter to the corresponding date-partition directory."""

    def __init__(self, base_output_dir: Path, cache_ttl: int = 60) -> None:
        self._base = Path(base_output_dir)
        self._cache_ttl = cache_ttl
        self._dates_cache: _DatesCache | None = None

    # ------------------------------------------------------------------
    # pt validation
    # ------------------------------------------------------------------

    def validate_pt(self, pt: str) -> bool:
        """Return *True* if *pt* is ``'_legacy'``, ``'_root'``, a valid ``yyyymmdd`` date, or ``yyyymmdd_yyyymmdd`` range."""
        if pt in ("_legacy", "_root"):
            return True
        if _YYYYMMDD_RE.match(pt):
            try:
                datetime.strptime(pt, "%Y%m%d")
                return True
            except ValueError:
                return False
        m = _DATE_RANGE_RE.match(pt)
        if m:
            try:
                datetime.strptime(m.group(1), "%Y%m%d")
                datetime.strptime(m.group(2), "%Y%m%d")
                return True
            except ValueError:
                return False
        return False

    # ------------------------------------------------------------------
    # directory resolution
    # ------------------------------------------------------------------

    def resolve_dir(self, pt: str) -> Path:
        """Return ``base_output_dir / pt``.  Does **not** check existence."""
        if pt == "_legacy":
            legacy_dir = self._base / "_legacy"
            if legacy_dir.is_dir():
                return legacy_dir
            return self._base
        if pt == "_root":
            return self._base
        return self._base / pt

    # ------------------------------------------------------------------
    # latest date
    # ------------------------------------------------------------------

    def get_latest_pt(self) -> str | None:
        """Scan *base_output_dir* for ``yyyymmdd`` directories and return the latest."""
        if not self._base.is_dir():
            return None

        latest: str | None = None
        for child in self._base.iterdir():
            if not child.is_dir():
                continue
            name = child.name
            if not _YYYYMMDD_RE.match(name):
                continue
            try:
                datetime.strptime(name, "%Y%m%d")
            except ValueError:
                continue
            if latest is None or name > latest:
                latest = name
        return latest

    # ------------------------------------------------------------------
    # service construction
    # ------------------------------------------------------------------

    def build_services(self, pt: str) -> dict[str, Any]:
        """Build service instances for the given *pt* partition."""
        partition_dir = self.resolve_dir(pt)
        if not partition_dir.is_dir():
            raise ValueError(f"Partition directory does not exist: {partition_dir}")

        sqlite_path = partition_dir / "pipeline_results.sqlite3"
        sql = sqlite_path if sqlite_path.is_file() else None

        return {
            "stats_service": StatsService(sqlite_path=sql),
            "run_service": RunService(sqlite_path=sql),
            "search_service": SearchService(sqlite_path=sql),
            "online_query_service": OnlineQueryService(partition_dir=partition_dir),
        }

    def build_online_query_service(self, pt: str) -> OnlineQueryService:
        """Build online query service for the given *pt*, creating the partition if needed."""
        partition_dir = self.resolve_dir(pt)
        partition_dir.mkdir(parents=True, exist_ok=True)
        return OnlineQueryService(partition_dir=partition_dir)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _read_record_count(partition_dir: Path) -> int:
        """Open the partition's SQLite file, read the row count, and close immediately."""
        db_path = partition_dir / "pipeline_results.sqlite3"
        if not db_path.is_file():
            return 0
        try:
            conn = sqlite3.connect(str(db_path))
            try:
                count = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
            except sqlite3.OperationalError:
                count = 0
            finally:
                conn.close()
            return count
        except Exception:
            logger.debug("Failed to read record count from %s", db_path, exc_info=True)
            return 0

    # ------------------------------------------------------------------
    # list dates
    # ------------------------------------------------------------------

    def list_dates(self) -> list[dict]:
        """Return available date partitions sorted descending, ``_legacy`` last."""
        if (
            self._dates_cache is not None
            and time.monotonic() < self._dates_cache.expires_at
        ):
            return self._dates_cache.entries

        if not self._base.is_dir():
            entries: list[dict] = []
            self._dates_cache = _DatesCache(
                entries=entries,
                latest_pt=None,
                expires_at=time.monotonic() + self._cache_ttl,
            )
            return entries

        date_entries: list[dict] = []
        range_entries: list[dict] = []
        has_legacy = False

        for child in self._base.iterdir():
            if not child.is_dir():
                continue
            name = child.name
            if name == "_legacy":
                has_legacy = True
                continue
            if _YYYYMMDD_RE.match(name):
                try:
                    datetime.strptime(name, "%Y%m%d")
                except ValueError:
                    continue
                record_count = self._read_record_count(child)
                date_entries.append({"pt": name, "record_count": record_count})
                continue
            m = _DATE_RANGE_RE.match(name)
            if m:
                try:
                    datetime.strptime(m.group(1), "%Y%m%d")
                    datetime.strptime(m.group(2), "%Y%m%d")
                except ValueError:
                    continue
                record_count = self._read_record_count(child)
                range_entries.append({"pt": name, "record_count": record_count})

        date_entries.sort(key=lambda e: e["pt"], reverse=True)
        range_entries.sort(key=lambda e: e["pt"].split("_")[1], reverse=True)

        latest_pt = date_entries[0]["pt"] if date_entries else None
        all_entries = date_entries + range_entries

        if has_legacy:
            legacy_dir = self._base / "_legacy"
            record_count = self._read_record_count(legacy_dir)
            all_entries.append({"pt": "_legacy", "record_count": record_count})

        self._dates_cache = _DatesCache(
            entries=all_entries,
            latest_pt=latest_pt,
            expires_at=time.monotonic() + self._cache_ttl,
        )
        return all_entries

    # ------------------------------------------------------------------
    # date range helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _date_range(pt_start: str, pt_end: str) -> list[str]:
        """Return all ``yyyymmdd`` strings from *pt_start* to *pt_end* inclusive."""
        start = datetime.strptime(pt_start, "%Y%m%d")
        end = datetime.strptime(pt_end, "%Y%m%d")
        days: list[str] = []
        cur = start
        while cur <= end:
            days.append(cur.strftime("%Y%m%d"))
            cur += timedelta(days=1)
        return days

    # ------------------------------------------------------------------
    # range aggregation
    # ------------------------------------------------------------------

    def aggregate_stats(self, pt_start: str, pt_end: str) -> StatsResponse:
        """Aggregate statistics across all existing partitions in the date range."""
        if pt_start > pt_end:
            raise ValueError(
                f"pt_start ({pt_start}) must not be later than pt_end ({pt_end})"
            )

        validity_type_dist: dict[str, int] = {}
        total_count = 0
        formal_count = 0
        fallback_count = 0

        for pt in self._date_range(pt_start, pt_end):
            partition_dir = self.resolve_dir(pt)
            if not partition_dir.is_dir():
                continue

            db_path = partition_dir / "pipeline_results.sqlite3"
            if not db_path.is_file():
                continue

            try:
                conn = sqlite3.connect(str(db_path))
                try:
                    # 按 route 统计
                    rows = conn.execute(
                        "SELECT route, COUNT(*) FROM pipeline_runs GROUP BY route"
                    ).fetchall()
                    for route, cnt in rows:
                        total_count += cnt
                        if route == "formal":
                            formal_count += cnt
                        elif route == "fallback":
                            fallback_count += cnt

                    # validity_type 分布（从 decision_record_json 提取）
                    decision_rows = conn.execute(
                        "SELECT decision_record_json FROM pipeline_runs WHERE decision_record_json IS NOT NULL"
                    ).fetchall()
                    for (dr_json,) in decision_rows:
                        try:
                            dr = json.loads(dr_json)
                            vt = dr.get("validity_type", "unknown")
                            validity_type_dist[vt] = validity_type_dist.get(vt, 0) + 1
                        except (json.JSONDecodeError, TypeError):
                            continue
                except sqlite3.OperationalError:
                    logger.debug("Failed to query stats from %s", db_path, exc_info=True)
                finally:
                    conn.close()
            except Exception:
                logger.debug("Failed to open SQLite at %s", db_path, exc_info=True)

        return StatsResponse(
            validity_type_distribution=validity_type_dist,
            total_count=total_count,
            formal_count=formal_count,
            fallback_count=fallback_count,
        )

    # ------------------------------------------------------------------
    # daily summaries
    # ------------------------------------------------------------------

    def list_daily_summaries(self, pt_start: str, pt_end: str) -> list[dict]:
        """Return per-day summaries for existing partitions in the date range."""
        if pt_start > pt_end:
            raise ValueError(
                f"pt_start ({pt_start}) must not be later than pt_end ({pt_end})"
            )

        summaries: list[dict] = []

        for pt in self._date_range(pt_start, pt_end):
            partition_dir = self.resolve_dir(pt)
            if not partition_dir.is_dir():
                continue

            summary_path = partition_dir / "run_summary.json"
            if summary_path.is_file():
                try:
                    data = json.loads(summary_path.read_text(encoding="utf-8"))
                    formal_c = data.get("formal_count", 0)
                    fallback_c = data.get("fallback_count", 0)
                    total_c = data.get("processed_count", formal_c + fallback_c)
                    summaries.append(
                        {
                            "pt": pt,
                            "total_count": total_c,
                            "formal_count": formal_c,
                            "fallback_count": fallback_c,
                        }
                    )
                    continue
                except Exception:
                    logger.debug("Failed to read run_summary.json from %s", summary_path, exc_info=True)

            db_path = partition_dir / "pipeline_results.sqlite3"
            if not db_path.is_file():
                continue

            try:
                conn = sqlite3.connect(str(db_path))
                try:
                    rows = conn.execute(
                        "SELECT route, COUNT(*) FROM pipeline_runs GROUP BY route"
                    ).fetchall()
                    formal_c = 0
                    fallback_c = 0
                    for route, cnt in rows:
                        if route == "formal":
                            formal_c = cnt
                        elif route == "fallback":
                            fallback_c = cnt
                    summaries.append(
                        {
                            "pt": pt,
                            "total_count": formal_c + fallback_c,
                            "formal_count": formal_c,
                            "fallback_count": fallback_c,
                        }
                    )
                except sqlite3.OperationalError:
                    logger.debug("Failed to query summaries from %s", db_path, exc_info=True)
                finally:
                    conn.close()
            except Exception:
                logger.debug("Failed to open SQLite at %s", db_path, exc_info=True)

        summaries.sort(key=lambda s: s["pt"], reverse=True)
        return summaries
