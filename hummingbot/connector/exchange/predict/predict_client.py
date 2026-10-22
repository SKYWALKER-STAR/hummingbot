"""Async REST transport shared by public data, auth and trading."""
import asyncio
import base64
import json
import logging
import os
import re
import time
from collections import deque
from urllib.parse import urlsplit

import aiohttp

logger = logging.getLogger(__name__)


class PredictAPIError(IOError):
    def __init__(self, status, path, *, safe_diagnostics=None):
        self.status = status
        # Only accept the whitelisted, redacted output of _log_api_failure.
        self.safe_diagnostics = dict(safe_diagnostics or {})
        message = f"Predict request failed: HTTP {status}, {path}"
        if self.safe_diagnostics:
            message += " diagnostics=" + json.dumps(self.safe_diagnostics, ensure_ascii=True)
        super().__init__(message)


class PredictClient:
    URLS = {"predict": "https://api.predict.fun", "predict_testnet": "https://api-testnet.predict.fun"}

    def __init__(self, api_key="", domain="predict", signer=None, proxy_url=None):
        if domain not in self.URLS:
            raise ValueError("Unknown Predict domain")
        self.proxy_url = (os.environ.get("PREDICT_PROXY_URL", "") if proxy_url is None else proxy_url).strip()
        if self.proxy_url:
            try:
                parsed = urlsplit(self.proxy_url)
                valid = (parsed.scheme in {"http", "https"} and parsed.hostname
                         and parsed.port != 0 and not parsed.query and not parsed.fragment
                         and parsed.path in {"", "/"})
            except ValueError:
                valid = False
            if not valid:
                raise ValueError("PREDICT_PROXY_URL must be an HTTP/HTTPS proxy URL")
        self.base_url = self.URLS[domain]
        self.api_key = api_key
        self.signer = signer
        self._session = None
        self._jwt = None
        self._expires = 0
        self._auth_lock = asyncio.Lock()
        self._limit_lock = asyncio.Lock()
        self._requests = deque()

    @property
    def headers(self):
        return {"x-api-key": self.api_key} if self.api_key else {}

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _throttle(self):
        async with self._limit_lock:
            while True:
                now = time.monotonic()
                while self._requests and now - self._requests[0] >= 60:
                    self._requests.popleft()
                # Leave margin below the documented 240/min account limit.
                if len(self._requests) < 220:
                    self._requests.append(now)
                    return
                await asyncio.sleep(max(0.01, 60 - (now - self._requests[0])))

    def _safe_diagnostics(self, payload, headers, *, token=None, data=None):
        # Whitelist diagnostic fields. Never serialize response/request objects.
        secrets = [self.api_key, self._jwt, token, self.proxy_url]
        if self.proxy_url:
            from urllib.parse import unquote
            proxy = urlsplit(self.proxy_url)
            secrets.extend(unquote(v) for v in (proxy.username, proxy.password) if v)

        def collect(value):
            if isinstance(value, dict):
                for item in value.values():
                    collect(item)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    collect(item)
            elif isinstance(value, str) and len(value) >= 8:
                secrets.append(value)

        collect(data)

        def clean(value):
            if not isinstance(value, (str, int)) or isinstance(value, bool):
                return None
            text = str(value)
            for secret in sorted({v for v in secrets if v}, key=len, reverse=True):
                text = text.replace(secret, "[REDACTED]")
            # Also cover unfamiliar credentials echoed by upstream services.
            text = re.sub(r"(?i)bearer\s+[^\s,;]+", "Bearer [REDACTED]", text)
            text = re.sub(r"[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+", "[REDACTED]", text)
            text = re.sub(r"(?i)0x[0-9a-f]{40,}|[A-Za-z0-9_+/=-]{48,}", "[REDACTED]", text)
            text = re.sub(r"(?i)(api[-_ ]?key|authorization|signature|private[-_ ]?key|token)\s*[:=]\s*[^,;\s]+",
                          r"\1=[REDACTED]", text)
            # Prevent log injection and terminal control sequences.
            return "".join(c if c.isprintable() else " " for c in text)[:512]

        result = {}
        for name in ("Content-Type", "X-Request-ID", "X-Correlation-ID", "CF-Ray", "Retry-After"):
            value = clean(headers.get(name))
            if value is not None:
                result[name.lower()] = value
        if isinstance(payload, dict):
            for name in ("code", "message", "error", "errorCode", "errorMessage"):
                value = clean(payload.get(name))
                if value is not None:
                    result[name] = value
            error = payload.get("error")
            if isinstance(error, dict):
                for name in ("code", "message"):
                    value = clean(error.get(name))
                    if value is not None:
                        result["error_" + name] = value
        return result

    async def _log_api_failure(self, response, method, path, *, token=None, data=None, payload=None):
        try:
            body_kind = "json"
            if payload is None:
                # Read at most 16 KiB; do not dump HTML gateways or oversized bodies.
                raw = bytearray()
                while len(raw) <= 16384:
                    chunk = await response.content.read(min(4096, 16385 - len(raw)))
                    if not chunk:
                        break
                    raw.extend(chunk)
                if len(raw) > 16384:
                    body_kind = "oversized"
                else:
                    try:
                        payload = json.loads(raw)
                    except (ValueError, UnicodeError):
                        body_kind = "non_json"
            details = self._safe_diagnostics(payload, response.headers, token=token, data=data)
            details["body_kind"] = body_kind
            # Paths can contain order hashes. Only log known endpoint roots.
            route = "/v1/" + path.split("/")[2].split("?")[0] if path.startswith("/v1/") else "unknown"
            if route not in {"/v1/orders", "/v1/auth", "/v1/markets", "/v1/positions", "/v1/account"}:
                route = "other"
            logger.warning("Predict API rejected request method=%s route=%s status=%s diagnostics=%s",
                           method if method in {"GET", "POST", "DELETE", "PUT"} else "OTHER",
                           route, response.status, json.dumps(details, ensure_ascii=True))
            return details
        except Exception:
            # Diagnostic failures must not hide the original status or leak exception text.
            logger.warning("Predict API error diagnostics unavailable status=%s", response.status)
            return {"body_kind": "unavailable"}

    def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self._session

    async def ws_connect(self, url, **kwargs):
        # WSConnection delegates here, so REST and both streams share routing.
        return await self._get_session().ws_connect(url, proxy=self.proxy_url or None, **kwargs)

    async def _send(self, method, path, *, params=None, data=None, token=None):
        await self._throttle()
        self._get_session()
        headers = self.headers
        if token:
            headers["Authorization"] = f"Bearer {token}"
        async with self._session.request(method, self.base_url + path, params=params,
                                         json=data, headers=headers, proxy=self.proxy_url or None) as response:
            if response.status >= 400:
                details = await self._log_api_failure(response, method, path, token=token, data=data)
                raise PredictAPIError(response.status, path, safe_diagnostics=details)
            payload = await response.json()
            if not isinstance(payload, dict) or payload.get("success") is not True:
                details = await self._log_api_failure(response, method, path, token=token, data=data, payload=payload)
                raise PredictAPIError(response.status, path, safe_diagnostics=details)
            return payload

    async def jwt(self):
        async with self._auth_lock:
            if self._jwt and time.time() < self._expires:
                return self._jwt
            if self.signer is None:
                raise ValueError("Private key required for authenticated requests")
            message = (await self._send("GET", "/v1/auth/message"))["data"]["message"]
            body = await asyncio.to_thread(self.signer.auth_message, message)
            token = (await self._send("POST", "/v1/auth", data=body))["data"]["token"]
            if not isinstance(token, str) or not token:
                raise ValueError("Invalid auth token")
            try:
                segment = token.split(".")[1]
                expiry = float(json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))["exp"])
            except (ValueError, KeyError, IndexError, TypeError):
                expiry = time.time() + 300
            self._jwt, self._expires = token, expiry - 30
            return token

    def invalidate_jwt(self):
        self._jwt, self._expires = None, 0

    async def request(self, method, path, *, params=None, data=None, private=False):
        token = await self.jwt() if private else None
        try:
            return await self._send(method, path, params=params, data=data, token=token)
        except PredictAPIError as exc:
            # Only retry a POST when the server explicitly rejected authentication.
            # An ambiguous timeout must be reconciled by order hash by the caller.
            if exc.status != 401 or not private:
                raise
            if token == self._jwt:
                self.invalidate_jwt()
            return await self._send(method, path, params=params, data=data, token=await self.jwt())

    async def pages(self, path, **params):
        rows, seen = [], set()
        for _ in range(1000):
            response = await self.request("GET", path, params=params, private=True)
            page = response["data"]
            if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
                raise ValueError("Invalid paginated response")
            rows.extend(page)
            cursor = response.get("cursor")
            if not cursor:
                return rows
            if not isinstance(cursor, str) or cursor in seen:
                raise ValueError("Repeated/invalid pagination cursor")
            seen.add(cursor)
            params["after"] = cursor
        raise ValueError("Pagination limit exceeded")
