"""
Middleware for request logging, rate limiting, and error tracking
"""

import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional
from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('yxdb-api')


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log all incoming requests with timing"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        
        # Log request
        logger.info(f"→ {request.method} {request.url.path}")
        
        try:
            response = await call_next(request)
            
            # Calculate duration
            duration = (time.time() - start_time) * 1000
            
            # Log response
            logger.info(
                f"← {request.method} {request.url.path} "
                f"[{response.status_code}] {duration:.2f}ms"
            )
            
            # Add timing header
            response.headers["X-Response-Time"] = f"{duration:.2f}ms"
            
            return response
            
        except Exception as e:
            duration = (time.time() - start_time) * 1000
            logger.error(
                f"✕ {request.method} {request.url.path} "
                f"[ERROR] {duration:.2f}ms - {str(e)}"
            )
            raise


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Simple in-memory rate limiting
    Note: For production, use Redis-based rate limiting
    """
    
    def __init__(
        self, 
        app: ASGIApp,
        requests_per_minute: int = 60,
        requests_per_hour: int = 1000
    ):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.minute_counts: Dict[str, List[float]] = defaultdict(list)
        self.hour_counts: Dict[str, List[float]] = defaultdict(list)
    
    def _get_client_id(self, request: Request) -> str:
        """Get client identifier from request"""
        # Try X-Forwarded-For first (for proxied requests)
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"
    
    def _clean_old_requests(self, timestamps: List[float], window_seconds: int) -> List[float]:
        """Remove timestamps older than the window"""
        cutoff = time.time() - window_seconds
        return [t for t in timestamps if t > cutoff]
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip rate limiting for health checks
        if request.url.path == "/api/health":
            return await call_next(request)
        
        client_id = self._get_client_id(request)
        now = time.time()
        
        # Clean and check minute rate
        self.minute_counts[client_id] = self._clean_old_requests(
            self.minute_counts[client_id], 60
        )
        if len(self.minute_counts[client_id]) >= self.requests_per_minute:
            logger.warning(f"Rate limit exceeded (minute) for {client_id}")
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded. Please wait before making more requests."
            )
        
        # Clean and check hour rate
        self.hour_counts[client_id] = self._clean_old_requests(
            self.hour_counts[client_id], 3600
        )
        if len(self.hour_counts[client_id]) >= self.requests_per_hour:
            logger.warning(f"Rate limit exceeded (hour) for {client_id}")
            raise HTTPException(
                status_code=429,
                detail="Hourly rate limit exceeded. Please try again later."
            )
        
        # Record this request
        self.minute_counts[client_id].append(now)
        self.hour_counts[client_id].append(now)
        
        response = await call_next(request)
        
        # Add rate limit headers
        response.headers["X-RateLimit-Limit"] = str(self.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(
            self.requests_per_minute - len(self.minute_counts[client_id])
        )
        
        return response


class ErrorTrackingMiddleware(BaseHTTPMiddleware):
    """
    Track and log errors for monitoring
    """
    
    def __init__(self, app: ASGIApp, max_errors: int = 100):
        super().__init__(app)
        self.errors: List[Dict] = []
        self.max_errors = max_errors
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            return await call_next(request)
        except Exception as e:
            # Log error
            error_info = {
                "timestamp": datetime.now().isoformat(),
                "method": request.method,
                "path": str(request.url.path),
                "error": str(e),
                "type": type(e).__name__
            }
            
            self.errors.append(error_info)
            
            # Keep only recent errors
            if len(self.errors) > self.max_errors:
                self.errors = self.errors[-self.max_errors:]
            
            logger.error(f"Unhandled error: {error_info}")
            
            raise
    
    def get_recent_errors(self, limit: int = 20) -> List[Dict]:
        """Get recent errors for monitoring"""
        return self.errors[-limit:]


# Metrics collector
class MetricsCollector:
    """Collect API metrics"""
    
    def __init__(self):
        self.request_counts: Dict[str, int] = defaultdict(int)
        self.response_times: Dict[str, List[float]] = defaultdict(list)
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.start_time = datetime.now()
    
    def record_request(self, path: str, method: str, status: int, duration: float):
        key = f"{method} {path}"
        self.request_counts[key] += 1
        self.response_times[key].append(duration)
        
        # Keep only last 1000 response times per endpoint
        if len(self.response_times[key]) > 1000:
            self.response_times[key] = self.response_times[key][-1000:]
        
        if status >= 400:
            self.error_counts[key] += 1
    
    def get_metrics(self) -> Dict:
        """Get current metrics"""
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        endpoint_stats = []
        for key, count in self.request_counts.items():
            times = self.response_times.get(key, [])
            endpoint_stats.append({
                "endpoint": key,
                "requests": count,
                "errors": self.error_counts.get(key, 0),
                "avg_response_ms": sum(times) / len(times) if times else 0,
                "p95_response_ms": sorted(times)[int(len(times) * 0.95)] if len(times) > 20 else 0
            })
        
        return {
            "uptime_seconds": uptime,
            "total_requests": sum(self.request_counts.values()),
            "total_errors": sum(self.error_counts.values()),
            "endpoints": sorted(endpoint_stats, key=lambda x: x["requests"], reverse=True)
        }


# Global metrics instance
metrics = MetricsCollector()
