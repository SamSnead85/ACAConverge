"""
API Gateway Routes for ACA DataHub
Rate limiting, circuit breaker, request transformation, and API management
"""

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import time
import hashlib

router = APIRouter(prefix="/gateway", tags=["API Gateway"])


# =========================================================================
# Models
# =========================================================================

class CircuitState(str, Enum):
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


class APIVersion(str, Enum):
    V1 = "v1"
    V2 = "v2"
    V2_1 = "v2.1"


# =========================================================================
# Rate Limiter
# =========================================================================

class RateLimiter:
    """Token bucket rate limiting"""
    
    def __init__(self):
        self.buckets: Dict[str, dict] = {}
        self.quotas: Dict[str, dict] = {}
    
    def check_rate(
        self,
        identifier: str,
        limit: int = 100,
        window_seconds: int = 60
    ) -> dict:
        now = time.time()
        bucket_key = f"{identifier}:{window_seconds}"
        
        if bucket_key not in self.buckets:
            self.buckets[bucket_key] = {
                "tokens": limit,
                "last_update": now
            }
        
        bucket = self.buckets[bucket_key]
        
        # Refill tokens
        elapsed = now - bucket["last_update"]
        refill_rate = limit / window_seconds
        bucket["tokens"] = min(limit, bucket["tokens"] + elapsed * refill_rate)
        bucket["last_update"] = now
        
        if bucket["tokens"] >= 1:
            bucket["tokens"] -= 1
            return {
                "allowed": True,
                "remaining": int(bucket["tokens"]),
                "reset_at": datetime.utcnow() + timedelta(seconds=window_seconds)
            }
        else:
            return {
                "allowed": False,
                "remaining": 0,
                "reset_at": datetime.utcnow() + timedelta(seconds=window_seconds),
                "retry_after": int(1 / refill_rate)
            }
    
    def set_quota(self, client_id: str, daily_limit: int, monthly_limit: int):
        self.quotas[client_id] = {
            "daily_limit": daily_limit,
            "monthly_limit": monthly_limit,
            "daily_used": 0,
            "monthly_used": 0,
            "daily_reset": (datetime.utcnow() + timedelta(days=1)).isoformat(),
            "monthly_reset": (datetime.utcnow() + timedelta(days=30)).isoformat()
        }
    
    def check_quota(self, client_id: str) -> dict:
        if client_id not in self.quotas:
            return {"allowed": True, "quota": None}
        
        quota = self.quotas[client_id]
        
        if quota["daily_used"] >= quota["daily_limit"]:
            return {"allowed": False, "reason": "daily_quota_exceeded"}
        
        if quota["monthly_used"] >= quota["monthly_limit"]:
            return {"allowed": False, "reason": "monthly_quota_exceeded"}
        
        quota["daily_used"] += 1
        quota["monthly_used"] += 1
        
        return {
            "allowed": True,
            "daily_remaining": quota["daily_limit"] - quota["daily_used"],
            "monthly_remaining": quota["monthly_limit"] - quota["monthly_used"]
        }


rate_limiter = RateLimiter()


# =========================================================================
# Circuit Breaker
# =========================================================================

class CircuitBreaker:
    """Circuit breaker for service protection"""
    
    def __init__(self):
        self.circuits: Dict[str, dict] = {}
        self.default_config = {
            "failure_threshold": 5,
            "success_threshold": 3,
            "timeout_seconds": 30
        }
    
    def get_circuit(self, service_name: str) -> dict:
        if service_name not in self.circuits:
            self.circuits[service_name] = {
                "state": CircuitState.CLOSED.value,
                "failure_count": 0,
                "success_count": 0,
                "last_failure": None,
                "opened_at": None,
                **self.default_config
            }
        return self.circuits[service_name]
    
    def can_execute(self, service_name: str) -> dict:
        circuit = self.get_circuit(service_name)
        
        if circuit["state"] == CircuitState.CLOSED.value:
            return {"allowed": True, "state": circuit["state"]}
        
        if circuit["state"] == CircuitState.OPEN.value:
            # Check if timeout has passed
            if circuit["opened_at"]:
                opened = datetime.fromisoformat(circuit["opened_at"])
                if (datetime.utcnow() - opened).seconds >= circuit["timeout_seconds"]:
                    circuit["state"] = CircuitState.HALF_OPEN.value
                    return {"allowed": True, "state": circuit["state"]}
            
            return {
                "allowed": False,
                "state": circuit["state"],
                "retry_after": circuit["timeout_seconds"]
            }
        
        # Half-open: allow limited requests
        return {"allowed": True, "state": circuit["state"]}
    
    def record_success(self, service_name: str):
        circuit = self.get_circuit(service_name)
        
        if circuit["state"] == CircuitState.HALF_OPEN.value:
            circuit["success_count"] += 1
            if circuit["success_count"] >= circuit["success_threshold"]:
                circuit["state"] = CircuitState.CLOSED.value
                circuit["failure_count"] = 0
                circuit["success_count"] = 0
        
        circuit["failure_count"] = max(0, circuit["failure_count"] - 1)
    
    def record_failure(self, service_name: str):
        circuit = self.get_circuit(service_name)
        circuit["failure_count"] += 1
        circuit["last_failure"] = datetime.utcnow().isoformat()
        
        if circuit["failure_count"] >= circuit["failure_threshold"]:
            circuit["state"] = CircuitState.OPEN.value
            circuit["opened_at"] = datetime.utcnow().isoformat()
            circuit["success_count"] = 0


circuit_breaker = CircuitBreaker()


# =========================================================================
# Request Transformer
# =========================================================================

class RequestTransformer:
    """Transform requests and responses"""
    
    def __init__(self):
        self.transformations: Dict[str, dict] = {}
    
    def add_transformation(self, route: str, config: dict):
        self.transformations[route] = config
    
    def transform_request(self, route: str, request: dict) -> dict:
        config = self.transformations.get(route, {})
        
        # Add headers
        if "add_headers" in config:
            request["headers"] = {**request.get("headers", {}), **config["add_headers"]}
        
        # Rename fields
        if "rename_fields" in config:
            for old_name, new_name in config["rename_fields"].items():
                if old_name in request.get("body", {}):
                    request["body"][new_name] = request["body"].pop(old_name)
        
        return request
    
    def transform_response(self, route: str, response: dict) -> dict:
        config = self.transformations.get(route, {})
        
        # Filter fields
        if "filter_response_fields" in config:
            allowed = set(config["filter_response_fields"])
            response = {k: v for k, v in response.items() if k in allowed}
        
        return response


request_transformer = RequestTransformer()


# =========================================================================
# API Version Manager
# =========================================================================

class APIVersionManager:
    """Manage API versions and deprecation"""
    
    def __init__(self):
        self.versions = {
            "v1": {
                "status": "deprecated",
                "deprecated_at": "2024-01-01",
                "sunset_at": "2025-06-01",
                "message": "Please migrate to v2"
            },
            "v2": {
                "status": "stable",
                "released_at": "2024-06-01"
            },
            "v2.1": {
                "status": "beta",
                "released_at": "2024-12-01"
            }
        }
    
    def check_version(self, version: str) -> dict:
        if version not in self.versions:
            return {"valid": False, "message": "Unknown API version"}
        
        info = self.versions[version]
        
        if info["status"] == "deprecated":
            return {
                "valid": True,
                "deprecated": True,
                "sunset_at": info.get("sunset_at"),
                "message": info.get("message")
            }
        
        return {"valid": True, "deprecated": False, "status": info["status"]}


version_manager = APIVersionManager()


# =========================================================================
# Analytics Collector
# =========================================================================

class APIAnalytics:
    """Collect API usage analytics"""
    
    def __init__(self):
        self.requests: List[dict] = []
        self.by_endpoint: Dict[str, int] = {}
        self.by_client: Dict[str, int] = {}
        self.errors: List[dict] = []
    
    def record(
        self,
        endpoint: str,
        method: str,
        client_id: str,
        status_code: int,
        latency_ms: float
    ):
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "endpoint": endpoint,
            "method": method,
            "client_id": client_id,
            "status_code": status_code,
            "latency_ms": latency_ms
        }
        
        self.requests.append(record)
        self.by_endpoint[endpoint] = self.by_endpoint.get(endpoint, 0) + 1
        self.by_client[client_id] = self.by_client.get(client_id, 0) + 1
        
        if status_code >= 400:
            self.errors.append(record)
        
        # Keep last 10000 records
        if len(self.requests) > 10000:
            self.requests = self.requests[-10000:]
    
    def get_summary(self) -> dict:
        if not self.requests:
            return {"total_requests": 0}
        
        latencies = [r["latency_ms"] for r in self.requests]
        
        return {
            "total_requests": len(self.requests),
            "unique_clients": len(self.by_client),
            "avg_latency_ms": round(sum(latencies) / len(latencies), 2),
            "error_rate": round(len(self.errors) / len(self.requests) * 100, 2),
            "top_endpoints": dict(sorted(self.by_endpoint.items(), key=lambda x: x[1], reverse=True)[:10]),
            "top_clients": dict(sorted(self.by_client.items(), key=lambda x: x[1], reverse=True)[:10])
        }


api_analytics = APIAnalytics()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/rate-limit/check")
async def check_rate_limit(
    client_id: str = Query(...),
    limit: int = Query(default=100),
    window: int = Query(default=60)
):
    """Check rate limit for client"""
    return rate_limiter.check_rate(client_id, limit, window)


@router.post("/quota/set")
async def set_quota(
    client_id: str = Query(...),
    daily_limit: int = Query(...),
    monthly_limit: int = Query(...)
):
    """Set quota for client"""
    rate_limiter.set_quota(client_id, daily_limit, monthly_limit)
    return {"success": True, "quota": rate_limiter.quotas[client_id]}


@router.get("/quota/{client_id}")
async def get_quota(client_id: str):
    """Get quota status for client"""
    return rate_limiter.check_quota(client_id)


@router.get("/circuit/{service_name}")
async def get_circuit_status(service_name: str):
    """Get circuit breaker status"""
    return circuit_breaker.get_circuit(service_name)


@router.post("/circuit/{service_name}/check")
async def check_circuit(service_name: str):
    """Check if circuit allows execution"""
    return circuit_breaker.can_execute(service_name)


@router.post("/circuit/{service_name}/success")
async def record_circuit_success(service_name: str):
    """Record successful execution"""
    circuit_breaker.record_success(service_name)
    return {"success": True, "circuit": circuit_breaker.get_circuit(service_name)}


@router.post("/circuit/{service_name}/failure")
async def record_circuit_failure(service_name: str):
    """Record failed execution"""
    circuit_breaker.record_failure(service_name)
    return {"success": True, "circuit": circuit_breaker.get_circuit(service_name)}


@router.get("/versions")
async def list_api_versions():
    """List all API versions"""
    return {"versions": version_manager.versions}


@router.get("/versions/{version}/check")
async def check_api_version(version: str):
    """Check API version status"""
    return version_manager.check_version(version)


@router.get("/analytics")
async def get_api_analytics():
    """Get API analytics summary"""
    return api_analytics.get_summary()


@router.post("/analytics/record")
async def record_api_call(
    endpoint: str = Query(...),
    method: str = Query(default="GET"),
    client_id: str = Query(...),
    status_code: int = Query(...),
    latency_ms: float = Query(...)
):
    """Record API call for analytics"""
    api_analytics.record(endpoint, method, client_id, status_code, latency_ms)
    return {"success": True}


@router.post("/transform/add")
async def add_transformation(
    route: str = Query(...),
    config: dict = None
):
    """Add request/response transformation"""
    request_transformer.add_transformation(route, config or {})
    return {"success": True}


@router.get("/mock/{endpoint:path}")
async def mock_endpoint(endpoint: str):
    """Mock API endpoint for sandbox testing"""
    return {
        "message": f"Mock response for /{endpoint}",
        "data": {"sample": "data"},
        "timestamp": datetime.utcnow().isoformat()
    }
