from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin, urlsplit

import httpx
from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from job_freshness.settings import _env_get


class AuthAuditStore:
    def __init__(self, path: str | Path | None = None) -> None:
        self._lock = threading.Lock()
        if path is None:
            self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        else:
            sqlite_path = Path(path)
            sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(sqlite_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                create table if not exists auth_events (
                    id integer primary key autoincrement,
                    event_type text not null,
                    open_id text not null,
                    name text not null default '',
                    email text not null default '',
                    enterprise_email text not null default '',
                    user_id text not null default '',
                    tenant_key text not null default '',
                    is_admin integer not null default 0,
                    created_at text not null default current_timestamp
                );

                create index if not exists idx_auth_events_open_id
                on auth_events (open_id);

                create table if not exists access_requests (
                    id integer primary key autoincrement,
                    open_id text not null,
                    name text not null default '',
                    email text not null default '',
                    enterprise_email text not null default '',
                    tenant_key text not null default '',
                    reason text not null default '',
                    status text not null default 'pending',
                    reviewer_note text not null default '',
                    created_at text not null default current_timestamp,
                    updated_at text not null default current_timestamp
                );

                create unique index if not exists idx_access_requests_open_id
                on access_requests (open_id);
                """
            )
            self._conn.commit()

    def record_event(self, event_type: str, user: "FeishuUser", is_admin: bool) -> None:
        with self._lock:
            self._conn.execute(
                """
                insert into auth_events (
                    event_type,
                    open_id,
                    name,
                    email,
                    enterprise_email,
                    user_id,
                    tenant_key,
                    is_admin
                ) values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_type,
                    user.open_id,
                    user.name,
                    user.email,
                    user.enterprise_email,
                    user.user_id,
                    user.tenant_key,
                    1 if is_admin else 0,
                ),
            )
            self._conn.commit()

    def list_recent_events(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                select event_type, open_id, name, email, enterprise_email, user_id, tenant_key, is_admin, created_at
                from auth_events
                order by id desc
                limit ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "event_type": row["event_type"],
                "open_id": row["open_id"],
                "name": row["name"],
                "email": row["email"],
                "enterprise_email": row["enterprise_email"],
                "user_id": row["user_id"],
                "tenant_key": row["tenant_key"],
                "is_admin": bool(row["is_admin"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def list_recent_users(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                select
                    latest.open_id,
                    latest.name,
                    latest.email,
                    latest.enterprise_email,
                    latest.user_id,
                    latest.tenant_key,
                    latest.is_admin,
                    latest.event_type as last_event_type,
                    latest.created_at as last_event_at,
                    summary.event_count
                from auth_events latest
                join (
                    select open_id, max(id) as last_id, count(*) as event_count
                    from auth_events
                    group by open_id
                ) summary
                  on latest.id = summary.last_id
                order by latest.id desc
                limit ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "open_id": row["open_id"],
                "name": row["name"],
                "email": row["email"],
                "enterprise_email": row["enterprise_email"],
                "user_id": row["user_id"],
                "tenant_key": row["tenant_key"],
                "is_admin": bool(row["is_admin"]),
                "last_event_type": row["last_event_type"],
                "last_event_at": row["last_event_at"],
                "event_count": row["event_count"],
            }
            for row in rows
        ]

    # -- access requests --------------------------------------------------

    def create_access_request(self, user: "FeishuUser", reason: str = "") -> dict[str, Any]:
        with self._lock:
            existing = self._conn.execute(
                "select id, status from access_requests where open_id = ?",
                (user.open_id,),
            ).fetchone()
            if existing:
                if existing["status"] == "pending":
                    return {"status": "already_pending"}
                if existing["status"] == "approved":
                    return {"status": "already_approved"}
                # rejected → allow re-apply
                self._conn.execute(
                    """
                    update access_requests
                    set status = 'pending', reason = ?, name = ?, email = ?,
                        enterprise_email = ?, tenant_key = ?,
                        reviewer_note = '', updated_at = current_timestamp
                    where open_id = ?
                    """,
                    (reason, user.name, user.email, user.enterprise_email, user.tenant_key, user.open_id),
                )
            else:
                self._conn.execute(
                    """
                    insert into access_requests (open_id, name, email, enterprise_email, tenant_key, reason)
                    values (?, ?, ?, ?, ?, ?)
                    """,
                    (user.open_id, user.name, user.email, user.enterprise_email, user.tenant_key, reason),
                )
            self._conn.commit()
        return {"status": "submitted"}

    def get_access_request_status(self, open_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "select status from access_requests where open_id = ?",
                (open_id,),
            ).fetchone()
        return row["status"] if row else None

    def list_access_requests(self, status_filter: str = "") -> list[dict[str, Any]]:
        with self._lock:
            if status_filter:
                rows = self._conn.execute(
                    "select * from access_requests where status = ? order by created_at desc",
                    (status_filter,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "select * from access_requests order by created_at desc"
                ).fetchall()
        return [
            {
                "open_id": row["open_id"],
                "name": row["name"],
                "email": row["email"],
                "enterprise_email": row["enterprise_email"],
                "tenant_key": row["tenant_key"],
                "reason": row["reason"],
                "status": row["status"],
                "reviewer_note": row["reviewer_note"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def update_access_request(self, open_id: str, status: str, reviewer_note: str = "") -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "select id from access_requests where open_id = ?",
                (open_id,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                """
                update access_requests
                set status = ?, reviewer_note = ?, updated_at = current_timestamp
                where open_id = ?
                """,
                (status, reviewer_note, open_id),
            )
            self._conn.commit()
        return {"open_id": open_id, "status": status}


class FeishuUser(BaseModel):
    model_config = ConfigDict(extra="ignore")

    open_id: str
    name: str = ""
    en_name: str = ""
    avatar_url: str = ""
    email: str = ""
    enterprise_email: str = ""
    user_id: str = ""
    tenant_key: str = ""


class FeishuAuthSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    app_id: str = ""
    app_secret: str = ""
    redirect_uri: str = ""
    frontend_base_url: str = "http://localhost:3000"
    authorize_url: str = "https://accounts.feishu.cn/open-apis/authen/v1/authorize"
    access_token_url: str = "https://open.feishu.cn/open-apis/authen/v1/access_token"
    user_info_url: str = "https://open.feishu.cn/open-apis/authen/v1/user_info"
    scope: str = "contact:user.base:readonly"
    session_secret: str = ""
    session_cookie_name: str = "job_freshness_session"
    session_ttl_sec: int = Field(default=8 * 60 * 60, ge=300)
    cookie_secure: bool = False
    cookie_domain: str = ""
    allowed_tenant_keys: list[str] = Field(default_factory=list)
    allowed_emails: list[str] = Field(default_factory=list)
    allowed_open_ids: list[str] = Field(default_factory=list)
    admin_emails: list[str] = Field(default_factory=list)
    admin_open_ids: list[str] = Field(default_factory=list)


def load_feishu_auth_settings() -> FeishuAuthSettings:
    return FeishuAuthSettings(
        enabled=_env_get("FEISHU_AUTH_ENABLED", "").lower() in {"1", "true", "yes", "on"},
        app_id=_env_get("FEISHU_APP_ID", ""),
        app_secret=_env_get("FEISHU_APP_SECRET", ""),
        redirect_uri=_env_get("FEISHU_REDIRECT_URI", ""),
        frontend_base_url=_env_get("FRONTEND_BASE_URL", "http://localhost:3000"),
        scope=_env_get("FEISHU_AUTH_SCOPE", "contact:user.base:readonly"),
        session_secret=_env_get("FEISHU_SESSION_SECRET", ""),
        session_cookie_name=_env_get("FEISHU_SESSION_COOKIE_NAME", "job_freshness_session"),
        session_ttl_sec=int(_env_get("FEISHU_SESSION_TTL_SEC", str(8 * 60 * 60))),
        cookie_secure=_env_get("FEISHU_COOKIE_SECURE", "").lower() in {"1", "true", "yes", "on"},
        cookie_domain=_env_get("FEISHU_COOKIE_DOMAIN", ""),
        allowed_tenant_keys=[item.strip() for item in _env_get("FEISHU_ALLOWED_TENANT_KEYS", "").split(",") if item.strip()],
        allowed_emails=[item.strip().lower() for item in _env_get("FEISHU_ALLOWED_EMAILS", "").split(",") if item.strip()],
        allowed_open_ids=[item.strip() for item in _env_get("FEISHU_ALLOWED_OPEN_IDS", "").split(",") if item.strip()],
        admin_emails=[item.strip().lower() for item in _env_get("FEISHU_ADMIN_EMAILS", "").split(",") if item.strip()],
        admin_open_ids=[item.strip() for item in _env_get("FEISHU_ADMIN_OPEN_IDS", "").split(",") if item.strip()],
    )


class FeishuAuthService:
    def __init__(
        self,
        settings: FeishuAuthSettings | None = None,
        http_client: httpx.Client | None = None,
        audit_store: AuthAuditStore | None = None,
    ) -> None:
        self._settings = settings or load_feishu_auth_settings()
        self._http_client = http_client
        self._audit_store = audit_store or AuthAuditStore()
        self._revoked_session_ids: set[str] = set()

    @property
    def enabled(self) -> bool:
        required = (
            self._settings.app_id,
            self._settings.app_secret,
            self._settings.redirect_uri,
            self._settings.session_secret,
        )
        return self._settings.enabled and all(required)

    @property
    def cookie_name(self) -> str:
        return self._settings.session_cookie_name

    @property
    def cookie_secure(self) -> bool:
        return self._settings.cookie_secure

    @property
    def cookie_domain(self) -> str:
        return self._settings.cookie_domain

    @property
    def session_ttl_sec(self) -> int:
        return self._settings.session_ttl_sec

    def _sign(self, raw: bytes) -> str:
        signature = hmac.new(
            self._settings.session_secret.encode("utf-8"),
            raw,
            hashlib.sha256,
        ).digest()
        return base64.urlsafe_b64encode(signature).decode("utf-8").rstrip("=")

    def _encode_token(self, payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        encoded = base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")
        signature = self._sign(raw)
        return f"{encoded}.{signature}"

    def _decode_token(self, token: str) -> dict[str, Any] | None:
        try:
            encoded, signature = token.split(".", 1)
            raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        except Exception:
            return None
        expected = self._sign(raw)
        if not hmac.compare_digest(signature, expected):
            return None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return None
        if payload.get("sid") in self._revoked_session_ids:
            return None
        if payload.get("exp", 0) < int(time.time()):
            return None
        return payload

    def _normalize_next_path(self, next_path: str | None) -> str:
        if not next_path or not next_path.startswith("/"):
            return "/"
        return next_path

    def build_login_url(self, next_path: str = "/") -> str:
        if not self.enabled:
            raise HTTPException(status_code=503, detail="Feishu auth is not configured")
        state = self._encode_token(
            {
                "next": self._normalize_next_path(next_path),
                "iat": int(time.time()),
                "exp": int(time.time()) + 10 * 60,
            }
        )
        query = urlencode(
            {
                "client_id": self._settings.app_id,
                "redirect_uri": self._settings.redirect_uri,
                "response_type": "code",
                "scope": self._settings.scope,
                "state": state,
            }
        )
        return f"{self._settings.authorize_url}?{query}"

    def create_session_cookie_value(self, user: FeishuUser) -> str:
        now = int(time.time())
        return self._encode_token(
            {
                "iat": now,
                "exp": now + self._settings.session_ttl_sec,
                "sid": self._build_session_id(user, now),
                "user": user.model_dump(),
            }
        )

    def clear_session_cookie_value(self) -> str:
        return ""

    def revoke_session(self, token: str | None) -> None:
        if not token:
            return
        payload = self._decode_token_without_revocation(token)
        if payload is None:
            return
        sid = payload.get("sid")
        if isinstance(sid, str) and sid:
            self._revoked_session_ids.add(sid)

    def get_current_user(self, request: Request) -> FeishuUser | None:
        if not self.enabled:
            return None
        cookie = request.cookies.get(self.cookie_name)
        if not cookie:
            return None
        payload = self._decode_token(cookie)
        if payload is None:
            return None
        user_payload = payload.get("user")
        if not isinstance(user_payload, dict):
            return None
        return FeishuUser.model_validate(user_payload)

    def _decode_token_without_revocation(self, token: str) -> dict[str, Any] | None:
        try:
            encoded, signature = token.split(".", 1)
            raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        except Exception:
            return None
        expected = self._sign(raw)
        if not hmac.compare_digest(signature, expected):
            return None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return None
        if payload.get("exp", 0) < int(time.time()):
            return None
        return payload

    def _build_session_id(self, user: FeishuUser, issued_at: int) -> str:
        raw = f"{user.open_id}:{issued_at}:{self._settings.session_secret}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def require_user(self, request: Request) -> FeishuUser | None:
        if not self.enabled:
            return None  # auth 关闭时放行
        user = self.get_current_user(request)
        if user is None:
            raise HTTPException(status_code=401, detail="Authentication required")
        if not self._is_allowed(user):
            raise HTTPException(status_code=403, detail="Access denied: your account is not authorized")
        return user

    def require_admin(self, request: Request) -> FeishuUser | None:
        if not self.enabled:
            return None  # auth 关闭时放行
        user = self.require_user(request)
        if not self.is_admin(user):
            raise HTTPException(status_code=403, detail="Admin access required")
        return user

    def is_admin(self, user: FeishuUser | None) -> bool:
        if user is None:
            return False
        has_admin_open_ids = bool(self._settings.admin_open_ids)
        has_admin_emails = bool(self._settings.admin_emails)
        if not has_admin_open_ids and not has_admin_emails:
            return True
        if has_admin_open_ids and user.open_id in self._settings.admin_open_ids:
            return True
        if has_admin_emails:
            email_candidates = {
                user.email.strip().lower(),
                user.enterprise_email.strip().lower(),
            }
            if any(email and email in self._settings.admin_emails for email in email_candidates):
                return True
        return False

    def get_session_payload(self, request: Request) -> dict[str, Any]:
        user = self.get_current_user(request)
        user_payload = None
        access_denied = False
        request_status = None
        if user is not None:
            user_payload = user.model_dump()
            user_payload["is_admin"] = self.is_admin(user)
            access_denied = not self._is_allowed(user)
            if access_denied:
                request_status = self._audit_store.get_access_request_status(user.open_id)
        return {
            "enabled": self.enabled,
            "authenticated": user is not None,
            "access_denied": access_denied,
            "request_status": request_status,
            "user": user_payload,
            "login_url": self.build_login_url("/") if self.enabled else None,
        }

    def get_admin_overview_payload(self) -> dict[str, Any]:
        frontend = self._settings.frontend_base_url
        redirect = self._settings.redirect_uri
        frontend_host = urlsplit(frontend).hostname or ""
        redirect_host = urlsplit(redirect).hostname or ""
        host_consistent = bool(frontend_host and redirect_host and frontend_host == redirect_host)
        warnings: list[str] = []
        if frontend_host and redirect_host and frontend_host != redirect_host:
            warnings.append("前端访问域名与飞书回调域名不一致，可能导致登录后会话无法共享。")
        if not self._settings.session_secret.strip():
            warnings.append("FEISHU_SESSION_SECRET 为空，认证会被视为未配置。")
        has_admin_allowlist = bool(self._settings.admin_open_ids or self._settings.admin_emails)
        has_access_allowlist = bool(self._settings.allowed_open_ids or self._settings.allowed_emails)
        return {
            "auth_enabled": self.enabled,
            "admin_mode": "allowlist" if has_admin_allowlist else "open_admin",
            "access_scope": "restricted" if has_access_allowlist else "all_authenticated",
            "frontend_base_url": frontend,
            "redirect_uri": redirect,
            "host_consistent": host_consistent,
            "allowed_open_id_count": len(self._settings.allowed_open_ids),
            "allowed_email_count": len(self._settings.allowed_emails),
            "admin_open_id_count": len(self._settings.admin_open_ids),
            "admin_email_count": len(self._settings.admin_emails),
            "warnings": warnings,
        }

    def get_access_settings_payload(self) -> dict[str, Any]:
        return {
            "allowed_open_ids": list(self._settings.allowed_open_ids),
            "allowed_emails": list(self._settings.allowed_emails),
            "admin_open_ids": list(self._settings.admin_open_ids),
            "admin_emails": list(self._settings.admin_emails),
        }

    def apply_access_settings(
        self,
        *,
        allowed_open_ids: list[str],
        allowed_emails: list[str],
        admin_open_ids: list[str],
        admin_emails: list[str],
    ) -> None:
        self._settings.allowed_open_ids = list(allowed_open_ids)
        self._settings.allowed_emails = list(allowed_emails)
        self._settings.admin_open_ids = list(admin_open_ids)
        self._settings.admin_emails = list(admin_emails)

    def record_auth_event(self, event_type: str, user: FeishuUser) -> None:
        self._audit_store.record_event(event_type, user, self.is_admin(user))

    def get_auth_audit_payload(self, event_limit: int = 20, user_limit: int = 20) -> dict[str, Any]:
        return {
            "events": self._audit_store.list_recent_events(event_limit),
            "users": self._audit_store.list_recent_users(user_limit),
        }

    def build_frontend_redirect(self, next_path: str) -> str:
        normalized = self._normalize_next_path(next_path)
        return urljoin(self._settings.frontend_base_url.rstrip("/") + "/", normalized.lstrip("/"))

    def _is_allowed(self, user: FeishuUser) -> bool:
        # 管理员始终放行
        if self.is_admin(user):
            return True
        # 1. tenant_key 白名单（配置了才检查）
        if self._settings.allowed_tenant_keys and user.tenant_key in self._settings.allowed_tenant_keys:
            return True
        # 2. open_id 白名单
        if self._settings.allowed_open_ids and user.open_id in self._settings.allowed_open_ids:
            return True
        # 3. email 白名单
        if self._settings.allowed_emails:
            email_candidates = {
                user.email.strip().lower(),
                user.enterprise_email.strip().lower(),
            }
            if any(email and email in self._settings.allowed_emails for email in email_candidates):
                return True
        # 4. 管理员手动批准
        req_status = self._audit_store.get_access_request_status(user.open_id)
        if req_status == "approved":
            return True
        # 默认拒绝
        return False

    def authenticate_with_code(self, code: str, state: str) -> tuple[FeishuUser, str]:
        if not self.enabled:
            raise HTTPException(status_code=503, detail="Feishu auth is not configured")
        state_payload = self._decode_token(state)
        if state_payload is None:
            raise HTTPException(status_code=400, detail="Invalid login state")
        next_path = self._normalize_next_path(state_payload.get("next"))
        user_access_token = self._exchange_code_for_access_token(code)
        user = self._fetch_user(user_access_token)
        print(f"[AUTH] Feishu login: name={user.name}, tenant_key={user.tenant_key}, open_id={user.open_id}, email={user.enterprise_email or user.email}")
        # 不再在此处 403 拒绝，而是让用户登录成功，由 session 携带 allowed 状态
        return user, next_path

    def _exchange_code_for_access_token(self, code: str) -> str:
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "app_id": self._settings.app_id,
            "app_secret": self._settings.app_secret,
        }
        client, should_close = self._client()
        try:
            resp = client.post(self._settings.access_token_url, json=payload)
        finally:
            if should_close:
                client.close()
        resp.raise_for_status()
        body = resp.json()
        if body.get("code") != 0:
            raise HTTPException(status_code=502, detail=body.get("msg", "Failed to exchange Feishu login code"))
        data = body.get("data") or {}
        token = data.get("access_token")
        if not token:
            raise HTTPException(status_code=502, detail="Feishu did not return a user access token")
        return token

    def _fetch_user(self, user_access_token: str) -> FeishuUser:
        client, should_close = self._client()
        try:
            resp = client.get(
                self._settings.user_info_url,
                headers={"Authorization": f"Bearer {user_access_token}"},
            )
        finally:
            if should_close:
                client.close()
        resp.raise_for_status()
        body = resp.json()
        if body.get("code") != 0:
            raise HTTPException(status_code=502, detail=body.get("msg", "Failed to fetch Feishu user info"))
        data = body.get("data") or {}
        return FeishuUser.model_validate(data)

    def _client(self) -> tuple[httpx.Client, bool]:
        if self._http_client is not None:
            return self._http_client, False
        return httpx.Client(timeout=10.0), True
