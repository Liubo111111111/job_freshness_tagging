"""API route definitions for the Freshness Pipeline Dashboard API.

Routes are defined WITHOUT the ``/api`` prefix — the prefix is applied
when the router is mounted in ``server.py``.

替换端点：/api/stats, /api/runs, /api/runs/{run_id}, /api/search
移除端点：/api/taxonomy, /api/classify/single, /api/classify/upload
复用端点：/api/auth/*, /api/dates, /api/daily-summary, /api/settings, /api/batch
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse

from job_freshness.api.auth import FeishuAuthService
from job_freshness.api.data_source_router import DataSourceRouter
from job_freshness.api.schemas import (
    AccessSettingsResponse,
    AccessSettingsUpdate,
    AdminAuthAuditResponse,
    AdminOverviewResponse,
    AnnotationRequest,
    AnnotationResponse,
    AuthSessionResponse,
    BatchAccepted,
    BatchRequest,
    OnlineQueryRequest,
    OnlineQueryResponse,
    SettingsUpdate,
)
from job_freshness.api.services import SettingsService


def _resolve_services(
    data_source_router: DataSourceRouter, pt: str | None
) -> dict[str, Any]:
    """Resolve *pt* to an effective partition and build service instances."""
    effective_pt = pt or data_source_router.get_latest_pt()
    if effective_pt is None:
        raise HTTPException(status_code=404, detail="No data partitions available")
    if not data_source_router.validate_pt(effective_pt):
        raise HTTPException(
            status_code=422,
            detail=f"Invalid pt format: {effective_pt}. Expected yyyymmdd or _legacy",
        )
    try:
        return data_source_router.build_services(effective_pt)
    except ValueError:
        raise HTTPException(
            status_code=404, detail=f"Data partition not found: {effective_pt}"
        )


def create_router(
    data_source_router: DataSourceRouter,
    settings_service: SettingsService,
    auth_service: FeishuAuthService | None = None,
    batch_trigger_fn: Callable[[BatchRequest, str], Any] | None = None,
    # 以下参数保留兼容性但不再使用
    taxonomy_service: Any = None,
    classify_single_fn: Any = None,
    classify_csv_fn: Any = None,
    classify_job_fn: Any = None,
) -> APIRouter:
    """Build and return an :class:`APIRouter` wired to the given services."""

    router = APIRouter()
    auth = auth_service

    def require_auth(request: Request):
        if auth is None:
            return None
        return auth.require_user(request)

    def require_admin(request: Request):
        if auth is None:
            return None
        return auth.require_admin(request)

    # ======================================================================
    # Auth 端点（复用）
    # ======================================================================

    @router.get("/auth/session", response_model=AuthSessionResponse)
    def get_auth_session(request: Request):
        if auth is None:
            return {
                "enabled": False,
                "authenticated": False,
                "access_denied": False,
                "request_status": None,
                "user": None,
                "login_url": None,
            }
        return auth.get_session_payload(request)

    @router.get("/admin/overview", response_model=AdminOverviewResponse)
    def get_admin_overview(_user=Depends(require_admin)):
        if auth is None:
            raise HTTPException(status_code=503, detail="Feishu auth is not configured")
        return auth.get_admin_overview_payload()

    @router.get("/admin/auth-audit", response_model=AdminAuthAuditResponse)
    def get_admin_auth_audit(_user=Depends(require_admin)):
        if auth is None:
            raise HTTPException(status_code=503, detail="Feishu auth is not configured")
        return auth.get_auth_audit_payload()

    @router.get("/admin/access-settings", response_model=AccessSettingsResponse)
    def get_admin_access_settings(_user=Depends(require_admin)):
        if auth is not None:
            current = settings_service.get_access_settings()
            approved = auth._audit_store.list_access_requests("approved")
            approved_ids = [r["open_id"] for r in approved if r.get("open_id")]
            missing = [oid for oid in approved_ids if oid not in current.allowed_open_ids]
            if missing:
                new_ids = current.allowed_open_ids + missing
                settings_service.update_access_settings(AccessSettingsUpdate(
                    allowed_open_ids=new_ids,
                    allowed_emails=current.allowed_emails,
                    admin_open_ids=current.admin_open_ids,
                    admin_emails=current.admin_emails,
                ))
                auth.apply_access_settings(
                    allowed_open_ids=new_ids,
                    allowed_emails=current.allowed_emails,
                    admin_open_ids=current.admin_open_ids,
                    admin_emails=current.admin_emails,
                )
            return auth.get_access_settings_payload()
        return settings_service.get_access_settings()

    @router.put("/admin/access-settings", response_model=AccessSettingsResponse)
    def update_admin_access_settings(update: AccessSettingsUpdate, _user=Depends(require_admin)):
        result = settings_service.update_access_settings(update)
        if auth is not None:
            auth.apply_access_settings(
                allowed_open_ids=result.allowed_open_ids,
                allowed_emails=result.allowed_emails,
                admin_open_ids=result.admin_open_ids,
                admin_emails=result.admin_emails,
            )
        return result

    @router.get("/auth/login")
    def login(next: str = Query("/", alias="next")):
        if auth is None:
            raise HTTPException(status_code=503, detail="Feishu auth is not configured")
        return RedirectResponse(auth.build_login_url(next))

    @router.get("/auth/callback")
    def auth_callback(code: str, state: str):
        if auth is None:
            raise HTTPException(status_code=503, detail="Feishu auth is not configured")
        user, next_path = auth.authenticate_with_code(code, state)
        auth.record_auth_event("login", user)
        response = RedirectResponse(auth.build_frontend_redirect(next_path), status_code=302)
        response.set_cookie(
            key=auth.cookie_name,
            value=auth.create_session_cookie_value(user),
            httponly=True,
            secure=auth.cookie_secure,
            samesite="lax",
            max_age=auth.session_ttl_sec,
            domain=auth.cookie_domain or None,
            path="/",
        )
        return response

    @router.post("/auth/logout")
    def logout(request: Request):
        if auth is None:
            return JSONResponse({"status": "logged_out"})
        current_user = auth.get_current_user(request)
        if current_user is not None:
            auth.record_auth_event("logout", current_user)
        auth.revoke_session(request.cookies.get(auth.cookie_name))
        response = JSONResponse({"status": "logged_out"})
        response.set_cookie(
            key=auth.cookie_name,
            value=auth.clear_session_cookie_value(),
            httponly=True,
            secure=auth.cookie_secure,
            samesite="lax",
            max_age=0,
            expires=0,
            domain=auth.cookie_domain or None,
            path="/",
        )
        return response

    # -- access requests --------------------------------------------------

    @router.post("/auth/request-access")
    def request_access(request: Request, body: dict = None):
        """用户提交访问权限申请"""
        if auth is None:
            raise HTTPException(status_code=503, detail="Auth not configured")
        user = auth.get_current_user(request)
        if user is None:
            raise HTTPException(status_code=401, detail="Authentication required")
        reason = (body or {}).get("reason", "") if body else ""
        result = auth._audit_store.create_access_request(user, reason)
        return result

    @router.get("/admin/access-requests")
    def list_access_requests(status: str = "", _user=Depends(require_admin)):
        if auth is None:
            raise HTTPException(status_code=503, detail="Auth not configured")
        return auth._audit_store.list_access_requests(status)

    @router.put("/admin/access-requests/{open_id}")
    def review_access_request(open_id: str, body: dict, _user=Depends(require_admin)):
        if auth is None:
            raise HTTPException(status_code=503, detail="Auth not configured")
        action = body.get("action", "")
        if action not in ("approve", "reject", "revoke"):
            raise HTTPException(status_code=400, detail="action must be 'approve', 'reject' or 'revoke'")

        if action == "revoke":
            status = "revoked"
        elif action == "approve":
            status = "approved"
        else:
            status = "rejected"

        reviewer_note = body.get("reviewer_note", "")
        result = auth._audit_store.update_access_request(open_id, status, reviewer_note)
        if result is None:
            raise HTTPException(status_code=404, detail="Access request not found")

        if action == "approve" and open_id:
            current = settings_service.get_access_settings()
            if open_id not in current.allowed_open_ids:
                new_ids = current.allowed_open_ids + [open_id]
                settings_service.update_access_settings(AccessSettingsUpdate(
                    allowed_open_ids=new_ids,
                    allowed_emails=current.allowed_emails,
                    admin_open_ids=current.admin_open_ids,
                    admin_emails=current.admin_emails,
                ))
                if auth is not None:
                    auth.apply_access_settings(
                        allowed_open_ids=new_ids,
                        allowed_emails=current.allowed_emails,
                        admin_open_ids=current.admin_open_ids,
                        admin_emails=current.admin_emails,
                    )

        if action == "revoke" and open_id:
            current = settings_service.get_access_settings()
            if open_id in current.allowed_open_ids:
                new_ids = [oid for oid in current.allowed_open_ids if oid != open_id]
                settings_service.update_access_settings(AccessSettingsUpdate(
                    allowed_open_ids=new_ids,
                    allowed_emails=current.allowed_emails,
                    admin_open_ids=current.admin_open_ids,
                    admin_emails=current.admin_emails,
                ))
                if auth is not None:
                    auth.apply_access_settings(
                        allowed_open_ids=new_ids,
                        allowed_emails=current.allowed_emails,
                        admin_open_ids=current.admin_open_ids,
                        admin_emails=current.admin_emails,
                    )

        return result

    # ======================================================================
    # Dates 端点（复用）
    # ======================================================================

    @router.get("/dates")
    def list_dates(_user=Depends(require_auth)):
        entries = data_source_router.list_dates()
        latest = data_source_router.get_latest_pt()
        return {"dates": entries, "latest_pt": latest}

    @router.get("/daily-summary")
    def get_daily_summary(
        pt_start: str = Query(...),
        pt_end: str = Query(...),
        _user=Depends(require_auth),
    ):
        if pt_start > pt_end:
            raise HTTPException(status_code=400, detail="pt_start must not be later than pt_end")
        return {"summaries": data_source_router.list_daily_summaries(pt_start, pt_end)}

    # ======================================================================
    # Stats 端点（替换：返回 temporal_status + signal_type 分布）
    # ======================================================================

    @router.get("/stats")
    def get_stats(
        pt: str = Query(None),
        pt_start: str = Query(None),
        pt_end: str = Query(None),
        _user=Depends(require_auth),
    ):
        # 日期范围查询优先
        if pt_start and pt_end:
            if not data_source_router.validate_pt(pt_start):
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid pt format: {pt_start}. Expected yyyymmdd or _legacy",
                )
            if not data_source_router.validate_pt(pt_end):
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid pt format: {pt_end}. Expected yyyymmdd or _legacy",
                )
            if pt_start > pt_end:
                raise HTTPException(status_code=400, detail="pt_start must not be later than pt_end")
            return data_source_router.aggregate_stats(pt_start, pt_end)

        services = _resolve_services(data_source_router, pt)
        return services["stats_service"].get_stats()

    # ======================================================================
    # Runs 端点（替换：返回新鲜度字段）
    # ======================================================================

    @router.get("/runs")
    def list_runs(
        offset: int = Query(0, ge=0),
        limit: int = Query(20, ge=1, le=100),
        pt: str = Query(None),
        annotation_status: str | None = Query(default=None, pattern="^(annotated|unannotated)$"),
        _user=Depends(require_auth),
    ):
        services = _resolve_services(data_source_router, pt)
        return services["run_service"].list_runs(offset, limit, annotation_status)

    @router.get("/runs/{run_id}")
    def get_run_detail(run_id: str, pt: str = Query(None), _user=Depends(require_auth)):
        services = _resolve_services(data_source_router, pt)
        detail = services["run_service"].get_run_detail(run_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return detail

    @router.put("/runs/{run_id}/annotation", response_model=AnnotationResponse)
    def annotate_run(
        run_id: str,
        req: AnnotationRequest,
        pt: str = Query(None),
        user=Depends(require_auth),
    ):
        services = _resolve_services(data_source_router, pt)
        reviewer_name = req.reviewer_name
        if not reviewer_name and user is not None:
            if isinstance(user, dict):
                reviewer_name = str(user.get("name") or user.get("en_name") or "")
            else:
                reviewer_name = str(
                    getattr(user, "name", "") or getattr(user, "en_name", "")
                )

        result = services["run_service"].annotate(
            run_id=run_id,
            annotated_label=req.annotated_label,
            reviewer_notes=req.reviewer_notes,
            reviewer_name=reviewer_name,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if result.status == "max_reached":
            raise HTTPException(status_code=400, detail="单条记录最多允许 3 次标注")
        return result

    # ======================================================================
    # Search 端点（替换：按 info_id 搜索）
    # ======================================================================

    @router.get("/search")
    def search(query: str = Query(..., min_length=1), pt: str = Query(None), _user=Depends(require_auth)):
        services = _resolve_services(data_source_router, pt)
        return services["search_service"].search(query)

    # ======================================================================
    # Online Query 端点（在线查询：按 info_id 实时查询 ODPS 并执行流水线）
    # ======================================================================

    @router.post("/query", response_model=OnlineQueryResponse)
    def online_query(req: OnlineQueryRequest, _user=Depends(require_auth)):
        """在线查询：按 info_id 实时查询 ODPS 宽表并返回最新运行结果。"""
        if not data_source_router.validate_pt(req.pt):
            raise HTTPException(
                status_code=422,
                detail=f"Invalid pt format: {req.pt}. Expected yyyymmdd or _legacy",
            )
        service = data_source_router.build_online_query_service(req.pt)
        return service.query(req.info_ids, req.pt)

    # ======================================================================
    # Batch 端点（复用）
    # ======================================================================

    @router.post("/batch", status_code=202)
    def trigger_batch(req: BatchRequest, _user=Depends(require_auth)):
        task_id = str(uuid.uuid4())
        if batch_trigger_fn:
            batch_trigger_fn(req, task_id)
        return BatchAccepted(task_id=task_id, message="Batch task accepted")

    # ======================================================================
    # Settings 端点（复用）
    # ======================================================================

    @router.get("/settings")
    def get_settings(_user=Depends(require_admin)):
        return settings_service.get_settings()

    @router.get("/settings/batch-config")
    def get_batch_config(_user=Depends(require_auth)):
        """返回批量配置（非管理员也可读取）"""
        s = settings_service.get_settings()
        return {"batch_max_rows": s.batch_max_rows}

    @router.put("/settings")
    def update_settings(update: SettingsUpdate, _user=Depends(require_admin)):
        return settings_service.update_settings(update)

    # ======================================================================
    # 以下端点已移除（不再提供行业分类功能）
    # - /api/taxonomy
    # - /api/classify/single
    # - /api/classify/upload
    # - /api/classify/by-job-name
    # - /api/classify/status/{task_id}
    # - /api/fallbacks
    # - /api/annotations
    # ======================================================================

    return router
