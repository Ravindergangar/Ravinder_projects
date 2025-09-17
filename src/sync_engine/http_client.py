from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class HttpError(Exception):
    pass


def _is_retryable(status: int) -> bool:
    return status in {408, 429, 500, 502, 503, 504}


class ResilientHttpClient:
    def __init__(self, base_url: str, default_headers: Optional[Dict[str, str]] = None, timeout_seconds: int = 30):
        self.base_url = base_url.rstrip("/")
        self.default_headers = default_headers or {}
        self.timeout_seconds = timeout_seconds

    @retry(reraise=True, stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=20), retry=retry_if_exception_type(HttpError))
    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        headers = {**self.default_headers, **kwargs.pop("headers", {})}
        response = requests.request(method=method, url=url, headers=headers, timeout=self.timeout_seconds, **kwargs)
        if _is_retryable(response.status_code):
            raise HttpError(f"Retryable HTTP {response.status_code}: {response.text[:200]}")
        if response.status_code >= 400:
            raise HttpError(f"HTTP {response.status_code}: {response.text[:200]}")
        return response

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        response = self._request("GET", path, params=params or {}, headers=headers or {})
        return response.json()

    def post_json(self, path: str, json: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        response = self._request("POST", path, json=json, headers=headers or {})
        return response.json()

    def patch_json(self, path: str, json: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        response = self._request("PATCH", path, json=json, headers=headers or {})
        return response.json()

