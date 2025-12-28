"""
Developer Platform API Routes for ACA DataHub
REST API v2, GraphQL, API keys, SDKs, and developer portal
"""

from fastapi import APIRouter, HTTPException, Query, Request, Depends
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import secrets
import hashlib
import json

router = APIRouter(prefix="/developer", tags=["Developer Platform"])


# =========================================================================
# Models
# =========================================================================

class APIKeyScope(BaseModel):
    name: str
    description: str
    permissions: List[str]


class APIKeyCreate(BaseModel):
    name: str
    scopes: List[str] = ["read"]
    expires_days: int = 365
    rate_limit: int = 1000  # requests per hour


# =========================================================================
# Developer Store
# =========================================================================

class DeveloperStore:
    """Manages API keys, usage, and developer resources"""
    
    def __init__(self):
        self.api_keys: Dict[str, dict] = {}
        self.usage: Dict[str, List[dict]] = {}
        self._key_counter = 0
        
        # Available scopes
        self.scopes = {
            "read": {"permissions": ["read:leads", "read:populations", "read:reports"]},
            "write": {"permissions": ["write:leads", "write:populations"]},
            "admin": {"permissions": ["admin:users", "admin:settings"]},
            "campaigns": {"permissions": ["read:campaigns", "write:campaigns", "send:campaigns"]},
            "analytics": {"permissions": ["read:analytics", "read:insights"]},
            "webhooks": {"permissions": ["manage:webhooks"]}
        }
        
        # Rate limit tiers
        self.rate_tiers = {
            "free": {"requests_per_hour": 100, "requests_per_day": 1000},
            "basic": {"requests_per_hour": 1000, "requests_per_day": 10000},
            "pro": {"requests_per_hour": 5000, "requests_per_day": 50000},
            "enterprise": {"requests_per_hour": 50000, "requests_per_day": 500000}
        }
    
    def create_key(self, user_id: str, data: dict) -> dict:
        self._key_counter += 1
        
        # Generate API key
        key_secret = secrets.token_urlsafe(32)
        key_id = f"ak_{self._key_counter}"
        key_prefix = key_secret[:8]
        key_hash = hashlib.sha256(key_secret.encode()).hexdigest()
        
        api_key = {
            "id": key_id,
            "user_id": user_id,
            "name": data.get("name", "API Key"),
            "key_prefix": key_prefix,
            "key_hash": key_hash,
            "scopes": data.get("scopes", ["read"]),
            "rate_limit": data.get("rate_limit", 1000),
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(days=data.get("expires_days", 365))).isoformat(),
            "last_used": None,
            "usage_count": 0,
            "is_active": True
        }
        
        self.api_keys[key_id] = api_key
        
        # Return with full key (only time it's shown)
        return {
            **api_key,
            "api_key": f"{key_id}_{key_secret}"
        }
    
    def validate_key(self, api_key: str) -> Optional[dict]:
        """Validate API key and return key info"""
        try:
            key_id, key_secret = api_key.split("_", 1)
            key_hash = hashlib.sha256(key_secret.encode()).hexdigest()
            
            if key_id in self.api_keys:
                stored_key = self.api_keys[key_id]
                if stored_key["key_hash"] == key_hash and stored_key["is_active"]:
                    # Check expiration
                    if stored_key["expires_at"] > datetime.utcnow().isoformat():
                        # Update usage
                        stored_key["last_used"] = datetime.utcnow().isoformat()
                        stored_key["usage_count"] += 1
                        return stored_key
        except:
            pass
        return None
    
    def list_keys(self, user_id: str) -> List[dict]:
        keys = [k for k in self.api_keys.values() if k["user_id"] == user_id]
        # Don't return hash
        return [{k: v for k, v in key.items() if k != "key_hash"} for key in keys]
    
    def revoke_key(self, key_id: str) -> bool:
        if key_id in self.api_keys:
            self.api_keys[key_id]["is_active"] = False
            return True
        return False
    
    def record_usage(self, key_id: str, endpoint: str, status_code: int):
        if key_id not in self.usage:
            self.usage[key_id] = []
        
        self.usage[key_id].append({
            "timestamp": datetime.utcnow().isoformat(),
            "endpoint": endpoint,
            "status_code": status_code
        })
        
        # Keep last 1000 entries
        if len(self.usage[key_id]) > 1000:
            self.usage[key_id] = self.usage[key_id][-1000:]
    
    def get_usage(self, key_id: str, days: int = 7) -> dict:
        entries = self.usage.get(key_id, [])
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        recent = [e for e in entries if e["timestamp"] > cutoff]
        
        return {
            "total_requests": len(recent),
            "by_endpoint": self._group_by_endpoint(recent),
            "by_status": self._group_by_status(recent),
            "period_days": days
        }
    
    def _group_by_endpoint(self, entries: List[dict]) -> dict:
        result = {}
        for e in entries:
            ep = e["endpoint"]
            result[ep] = result.get(ep, 0) + 1
        return result
    
    def _group_by_status(self, entries: List[dict]) -> dict:
        result = {}
        for e in entries:
            status = str(e["status_code"])
            result[status] = result.get(status, 0) + 1
        return result


developer_store = DeveloperStore()


# =========================================================================
# OpenAPI Spec Generator
# =========================================================================

def generate_openapi_spec() -> dict:
    """Generate OpenAPI 3.0 specification"""
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "ACA DataHub API",
            "description": "Enterprise data analytics and lead management API",
            "version": "2.0.0",
            "contact": {
                "name": "API Support",
                "email": "api@aca-datahub.com"
            }
        },
        "servers": [
            {"url": "https://api.aca-datahub.com/v2", "description": "Production"},
            {"url": "https://sandbox.aca-datahub.com/v2", "description": "Sandbox"}
        ],
        "security": [{"ApiKeyAuth": []}],
        "components": {
            "securitySchemes": {
                "ApiKeyAuth": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-API-Key"
                }
            }
        },
        "paths": {
            "/leads": {
                "get": {
                    "summary": "List leads",
                    "tags": ["Leads"],
                    "parameters": [
                        {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 100}},
                        {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}}
                    ],
                    "responses": {"200": {"description": "List of leads"}}
                }
            },
            "/leads/{id}": {
                "get": {
                    "summary": "Get lead by ID",
                    "tags": ["Leads"],
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "Lead details"}}
                }
            },
            "/populations": {
                "get": {"summary": "List populations", "tags": ["Populations"]},
                "post": {"summary": "Create population", "tags": ["Populations"]}
            },
            "/campaigns": {
                "get": {"summary": "List campaigns", "tags": ["Campaigns"]},
                "post": {"summary": "Create campaign", "tags": ["Campaigns"]}
            },
            "/analytics/metrics": {
                "get": {"summary": "Get analytics metrics", "tags": ["Analytics"]}
            }
        }
    }


# =========================================================================
# SDK Code Generators
# =========================================================================

def generate_python_sdk() -> str:
    return '''"""
ACA DataHub Python SDK
pip install aca-datahub
"""

import requests
from typing import Optional, List, Dict

class ACADataHub:
    def __init__(self, api_key: str, base_url: str = "https://api.aca-datahub.com/v2"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key})
    
    def list_leads(self, limit: int = 100, offset: int = 0) -> Dict:
        response = self.session.get(f"{self.base_url}/leads", params={"limit": limit, "offset": offset})
        response.raise_for_status()
        return response.json()
    
    def get_lead(self, lead_id: str) -> Dict:
        response = self.session.get(f"{self.base_url}/leads/{lead_id}")
        response.raise_for_status()
        return response.json()
    
    def create_population(self, name: str, query: str) -> Dict:
        response = self.session.post(f"{self.base_url}/populations", json={"name": name, "query": query})
        response.raise_for_status()
        return response.json()
    
    def send_campaign(self, campaign_id: str) -> Dict:
        response = self.session.post(f"{self.base_url}/campaigns/{campaign_id}/send")
        response.raise_for_status()
        return response.json()

# Usage:
# client = ACADataHub("your-api-key")
# leads = client.list_leads(limit=50)
'''


def generate_javascript_sdk() -> str:
    return '''/**
 * ACA DataHub JavaScript SDK
 * npm install @aca-datahub/sdk
 */

class ACADataHub {
  constructor(apiKey, baseUrl = 'https://api.aca-datahub.com/v2') {
    this.apiKey = apiKey;
    this.baseUrl = baseUrl;
  }

  async request(endpoint, options = {}) {
    const response = await fetch(`${this.baseUrl}${endpoint}`, {
      ...options,
      headers: {
        'X-API-Key': this.apiKey,
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });
    if (!response.ok) throw new Error(`API Error: ${response.status}`);
    return response.json();
  }

  async listLeads(limit = 100, offset = 0) {
    return this.request(`/leads?limit=${limit}&offset=${offset}`);
  }

  async getLead(leadId) {
    return this.request(`/leads/${leadId}`);
  }

  async createPopulation(name, query) {
    return this.request('/populations', {
      method: 'POST',
      body: JSON.stringify({ name, query }),
    });
  }

  async sendCampaign(campaignId) {
    return this.request(`/campaigns/${campaignId}/send`, { method: 'POST' });
  }
}

// Usage:
// const client = new ACADataHub('your-api-key');
// const leads = await client.listLeads(50);

export default ACADataHub;
'''


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/api-keys")
async def create_api_key(
    key_data: APIKeyCreate,
    user_id: str = Query(...)
):
    """Create a new API key"""
    result = developer_store.create_key(user_id, key_data.dict())
    return {
        "success": True,
        "key": result,
        "warning": "Save this API key - it won't be shown again!"
    }


@router.get("/api-keys")
async def list_api_keys(user_id: str = Query(...)):
    """List user's API keys"""
    return {"keys": developer_store.list_keys(user_id)}


@router.delete("/api-keys/{key_id}")
async def revoke_api_key(key_id: str):
    """Revoke an API key"""
    if not developer_store.revoke_key(key_id):
        raise HTTPException(status_code=404, detail="API key not found")
    return {"success": True}


@router.get("/api-keys/{key_id}/usage")
async def get_key_usage(key_id: str, days: int = Query(default=7, le=30)):
    """Get API key usage statistics"""
    return developer_store.get_usage(key_id, days)


@router.get("/scopes")
async def list_scopes():
    """List available API scopes"""
    return {"scopes": developer_store.scopes}


@router.get("/rate-limits")
async def get_rate_limits():
    """Get rate limit tiers"""
    return {"tiers": developer_store.rate_tiers}


@router.get("/openapi.json")
async def get_openapi_spec():
    """Get OpenAPI 3.0 specification"""
    return generate_openapi_spec()


@router.get("/sdk/python")
async def get_python_sdk():
    """Get Python SDK code"""
    return {"language": "python", "code": generate_python_sdk()}


@router.get("/sdk/javascript")
async def get_javascript_sdk():
    """Get JavaScript SDK code"""
    return {"language": "javascript", "code": generate_javascript_sdk()}


@router.post("/validate-key")
async def validate_api_key(api_key: str = Query(...)):
    """Validate an API key"""
    key_info = developer_store.validate_key(api_key)
    if not key_info:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    return {
        "valid": True,
        "scopes": key_info["scopes"],
        "rate_limit": key_info["rate_limit"],
        "expires_at": key_info["expires_at"]
    }


@router.post("/webhook-signature")
async def generate_webhook_signature(payload: dict, secret: str = Query(...)):
    """Generate webhook signature for payload"""
    payload_str = json.dumps(payload, sort_keys=True)
    signature = hashlib.sha256((payload_str + secret).encode()).hexdigest()
    
    return {
        "signature": f"sha256={signature}",
        "header_name": "X-Webhook-Signature"
    }


@router.get("/docs")
async def get_api_documentation():
    """Get API documentation links"""
    return {
        "docs": {
            "getting_started": "/developer/docs/getting-started",
            "authentication": "/developer/docs/authentication",
            "rate_limits": "/developer/docs/rate-limits",
            "webhooks": "/developer/docs/webhooks",
            "sdks": "/developer/docs/sdks",
            "changelog": "/developer/docs/changelog"
        },
        "openapi": "/developer/openapi.json",
        "playground": "/developer/playground"
    }


@router.get("/playground")
async def get_playground_config():
    """Get API playground configuration"""
    return {
        "endpoints": [
            {"method": "GET", "path": "/leads", "description": "List leads"},
            {"method": "GET", "path": "/populations", "description": "List populations"},
            {"method": "POST", "path": "/campaigns", "description": "Create campaign"},
            {"method": "GET", "path": "/analytics/metrics", "description": "Get metrics"}
        ],
        "sandbox_key": "sandbox_test_key_12345",
        "base_url": "https://sandbox.aca-datahub.com/v2"
    }
