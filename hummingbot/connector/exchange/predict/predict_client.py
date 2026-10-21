"""Async REST transport shared by public data, auth and trading."""
import asyncio
import base64
import json
import time
from collections import deque

import aiohttp


class PredictAPIError(IOError):
    def __init__(self, status, path):
        self.status = status
        # Never include response bodies, JWTs or signed orders in exceptions.
        super().__init__(f"Predict request failed: HTTP {status}, {path}")


class PredictClient:
    URLS = {"predict": "https://api.predict.fun", "predict_testnet": "https://api-testnet.predict.fun"}

    def __init__(self, api_key="", domain="predict", signer=None):
        if domain not in self.URLS:
            raise ValueError("Unknown Predict domain")
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

    async def _send(self, method, path, *, params=None, data=None, token=None):
        await self._throttle()
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        headers = self.headers
        if token:
            headers["Authorization"] = f"Bearer {token}"
        async with self._session.request(method, self.base_url + path, params=params,
                                         json=data, headers=headers) as response:
            if response.status >= 400:
                raise PredictAPIError(response.status, path)
            payload = await response.json()
            if not isinstance(payload, dict) or payload.get("success") is not True:
                raise PredictAPIError(response.status, path)
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
