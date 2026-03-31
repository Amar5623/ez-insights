"""
api/middleware/rate_limit.py

Sliding-window in-memory rate limiter.

Limits requests per IP per time window. Configurable via settings:
    RATE_LIMIT_REQUESTS  — max requests per window (default: 30)
    RATE_LIMIT_WINDOW_S  — window size in seconds (default: 60)

Returns 429 Too Many Requests with a Retry-After header when exceeded.
Single-instance only — not distributed. For multi-instance, replace
_request_log with a Redis sorted set.
"""

import time
from collections import defaultdict, deque
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from core.config.settings import get_settings
from core.logging_config import get_logger

logger = get_logger(__name__)

# IP → deque of timestamps (sliding window)
_request_log: dict[str, deque] = defaultdict(deque)


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        limit = getattr(settings, "RATE_LIMIT_REQUESTS", 30)
        window = getattr(settings, "RATE_LIMIT_WINDOW_S", 60)

        # Only rate-limit the query endpoint — health/docs are exempt
        if request.url.path != "/api/query":
            return await call_next(request)

        ip = request.client.host if request.client else "unknown"
        now = time.time()
        log = _request_log[ip]

        # Evict timestamps outside the window
        while log and log[0] < now - window:
            log.popleft()

        if len(log) >= limit:
            retry_after = int(window - (now - log[0])) + 1
            logger.warning(
                f"[RATE_LIMIT] Blocked | ip={ip} | "
                f"requests={len(log)} | window={window}s"
            )
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Too many requests. Max {limit} per {window}s."
                },
                headers={"Retry-After": str(retry_after)},
            )

        log.append(now)
        return await call_next(request)