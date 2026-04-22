from __future__ import annotations

import logging
import time
from typing import Any, Protocol

import httpx

from job_freshness.settings import LLMSettings, load_llm_settings

logger = logging.getLogger(__name__)


class LLMClient(Protocol):
    def complete(self, prompt: str, payload: dict) -> str:
        """Return raw model output text."""


class HttpLLMClient:
    """通过 OpenAI 兼容 HTTP 接口调用 LLM。"""

    def __init__(self, settings: LLMSettings | None = None) -> None:
        self._settings = settings or load_llm_settings()
        if not self._settings.api_key:
            raise ValueError(
                "LLM API key not configured. "
                "Set DASHSCOPE_API_KEY, OPENAI_API_KEY, or LLM_API_KEY in .env"
            )
        if not self._settings.base_url:
            raise ValueError(
                "LLM base URL not configured. "
                "Set LLM_BASE_URL, DASHSCOPE_BASE_URL, or OPENAI_BASE_URL in .env"
            )
        self._client = httpx.Client(
            timeout=httpx.Timeout(self._settings.timeout_sec, connect=10.0),
            trust_env=False,
        )

    def complete(self, prompt: str, payload: dict) -> str:
        """发送 prompt 到 LLM，返回原始文本输出。"""
        messages = self._build_messages(prompt)
        last_error: Exception | None = None

        for attempt in range(self._settings.max_retry + 1):
            try:
                response = self._call_api(messages)
                text = self._extract_text(response)
                logger.debug(
                    "llm_complete ok attempt=%d model=%s chars=%d",
                    attempt, self._settings.model, len(text),
                )
                return text
            except httpx.HTTPStatusError as exc:
                last_error = exc
                if not self._should_retry_http_status(exc.response.status_code):
                    raise RuntimeError(
                        f"llm_call_failed status={exc.response.status_code} "
                        f"body={self._response_excerpt(exc.response)}"
                    ) from exc
                wait = min(2 ** attempt, 8)
                logger.warning(
                    "llm_complete retry attempt=%d status=%d error=%s wait=%ds",
                    attempt, exc.response.status_code, type(exc).__name__, wait,
                )
                time.sleep(wait)
            except httpx.TimeoutException as exc:
                last_error = exc
                wait = min(2 ** attempt, 8)
                logger.warning(
                    "llm_complete retry attempt=%d error=%s wait=%ds",
                    attempt, type(exc).__name__, wait,
                )
                time.sleep(wait)
            except Exception as exc:
                raise RuntimeError(f"llm_call_failed: {exc}") from exc

        raise RuntimeError(
            "llm_call_exhausted_retries after "
            f"{self._settings.max_retry + 1} attempts: {self._format_error(last_error)}"
        )

    def _build_messages(self, prompt: str) -> list[dict[str, str]]:
        parts = prompt.split("[USER]\n", maxsplit=1)
        if len(parts) == 2:
            system_part = parts[0]
            user_part = parts[1]
            system_text = system_part.replace("[TASK:", "").split("]\n", 1)[-1]
            system_text = system_text.replace("[SYSTEM]\n", "").strip()
            return [
                {"role": "system", "content": system_text},
                {"role": "user", "content": user_part.strip()},
            ]
        return [{"role": "user", "content": prompt}]

    def _call_api(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        body = {
            "model": self._settings.model,
            "messages": messages,
            "temperature": 0.1,
        }
        headers = {
            "Authorization": f"Bearer {self._settings.api_key}",
            "Content-Type": "application/json",
        }
        resp = self._client.post(
            self._settings.base_url,
            json=body,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _should_retry_http_status(status_code: int) -> bool:
        return status_code == 429 or status_code >= 500

    @staticmethod
    def _response_excerpt(response: httpx.Response, limit: int = 200) -> str:
        text = response.text.strip()
        if not text:
            return "<empty>"
        return text[:limit]

    @classmethod
    def _format_error(cls, error: Exception | None) -> str:
        if isinstance(error, httpx.HTTPStatusError):
            return (
                f"status={error.response.status_code} "
                f"body={cls._response_excerpt(error.response)}"
            )
        return str(error)

    @staticmethod
    def _extract_text(response: dict[str, Any]) -> str:
        choices = response.get("choices", [])
        if not choices:
            raise ValueError("llm_response_no_choices")
        message = choices[0].get("message", {})
        content = message.get("content", "")
        if not content:
            raise ValueError("llm_response_empty_content")
        return content

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpLLMClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
