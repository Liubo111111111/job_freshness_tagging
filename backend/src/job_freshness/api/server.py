"""FastAPI application entry point.

Start with::

    uvicorn job_freshness.api.server:app --reload --port 8000
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from job_freshness.api.auth import AuthAuditStore, FeishuAuthService, load_feishu_auth_settings
from job_freshness.api.data_source_router import DataSourceRouter
from job_freshness.api.routes import create_router
from job_freshness.api.services import SettingsService

logger = logging.getLogger(__name__)

# Default output directory – backend/output/（与 CLI 默认 --output-dir 一致）
# server.py → api/ → job_freshness/ → src/ → backend/
_OUTPUT_DIR = Path(__file__).resolve().parents[3] / "output"


def create_app(output_dir: Path | None = None) -> FastAPI:
    """Build a fully-wired :class:`FastAPI` application."""

    out = output_dir or _OUTPUT_DIR
    out.mkdir(parents=True, exist_ok=True)

    # -- data source router -----------------------------------------------
    router_instance = DataSourceRouter(base_output_dir=out)

    # -- services ---------------------------------------------------------
    settings_svc = SettingsService()
    auth_settings = load_feishu_auth_settings()
    # SQLite for auth audit — use the base output dir
    auth_sqlite_path = out / "auth_audit.sqlite3"
    auth_svc = FeishuAuthService(auth_settings, audit_store=AuthAuditStore(auth_sqlite_path))

    # -- batch trigger (background thread) --------------------------------
    def batch_trigger_fn(req, task_id: str) -> None:
        def _run() -> None:
            logger.info("Batch task %s started (pt=%s, input_path=%s, workers=%d)", task_id, req.pt, req.input_path, req.worker_count)
            logger.info("Batch task %s finished", task_id)
        thread = threading.Thread(target=_run, daemon=True, name=f"batch-{task_id}")
        thread.start()

    # -- router -----------------------------------------------------------
    router = create_router(
        data_source_router=router_instance,
        settings_service=settings_svc,
        auth_service=auth_svc,
        batch_trigger_fn=batch_trigger_fn,
    )

    # -- app --------------------------------------------------------------
    app = FastAPI(title="Freshness Pipeline API", version="2.0.0")
    allow_origins = ["http://localhost:7070"]
    parsed_frontend = urlsplit(auth_settings.frontend_base_url)
    frontend_origin = ""
    if parsed_frontend.scheme and parsed_frontend.netloc:
        frontend_origin = f"{parsed_frontend.scheme}://{parsed_frontend.netloc}"
    if frontend_origin and frontend_origin not in allow_origins:
        allow_origins.append(frontend_origin)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router, prefix="/api")

    return app


app = create_app()
