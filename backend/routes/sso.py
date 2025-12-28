"""
Enterprise SSO & Identity Routes for ACA DataHub
SAML, OIDC, SCIM provisioning, and advanced identity management
"""

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import base64
import hashlib
import secrets

router = APIRouter(prefix="/sso", tags=["Enterprise SSO"])


# =========================================================================
# Models
# =========================================================================

class SSOProvider(str, Enum):
    SAML = "saml"
    OIDC = "oidc"
    LDAP = "ldap"


class SSOConfig(BaseModel):
    provider: SSOProvider
    name: str
    entity_id: Optional[str] = None  # SAML
    sso_url: Optional[str] = None  # SAML login URL
    certificate: Optional[str] = None  # SAML cert
    client_id: Optional[str] = None  # OIDC
    client_secret: Optional[str] = None  # OIDC
    issuer_url: Optional[str] = None  # OIDC
    ldap_url: Optional[str] = None  # LDAP
    base_dn: Optional[str] = None  # LDAP
    bind_dn: Optional[str] = None  # LDAP
    is_active: bool = True


class SCIMUser(BaseModel):
    userName: str
    name: Dict[str, str]
    emails: List[Dict[str, Any]]
    active: bool = True
    externalId: Optional[str] = None


# =========================================================================
# SSO Store
# =========================================================================

class SSOStore:
    """Manages SSO configurations and sessions"""
    
    def __init__(self):
        self.configs: Dict[str, dict] = {}
        self.sso_sessions: Dict[str, dict] = {}
        self.ip_allowlist: Dict[str, List[str]] = {}
        self._counter = 0
    
    def create_config(self, data: dict) -> dict:
        self._counter += 1
        config_id = f"sso_{self._counter}"
        
        config = {
            "id": config_id,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            **data
        }
        
        # Generate SP metadata for SAML
        if data.get("provider") == SSOProvider.SAML.value:
            config["sp_entity_id"] = f"urn:aca-datahub:sp:{config_id}"
            config["acs_url"] = f"/api/sso/saml/{config_id}/acs"
            config["metadata_url"] = f"/api/sso/saml/{config_id}/metadata"
        
        self.configs[config_id] = config
        return config
    
    def get_config(self, config_id: str) -> Optional[dict]:
        return self.configs.get(config_id)
    
    def list_configs(self, org_id: str = None) -> List[dict]:
        configs = list(self.configs.values())
        if org_id:
            configs = [c for c in configs if c.get("org_id") == org_id]
        return configs
    
    def update_config(self, config_id: str, updates: dict) -> Optional[dict]:
        if config_id not in self.configs:
            return None
        updates["updated_at"] = datetime.utcnow().isoformat()
        self.configs[config_id].update(updates)
        return self.configs[config_id]
    
    def delete_config(self, config_id: str) -> bool:
        if config_id in self.configs:
            del self.configs[config_id]
            return True
        return False
    
    def create_sso_session(self, user_id: str, provider: str, claims: dict) -> dict:
        session_id = secrets.token_urlsafe(32)
        
        session = {
            "id": session_id,
            "user_id": user_id,
            "provider": provider,
            "claims": claims,
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(hours=8)).isoformat()
        }
        
        self.sso_sessions[session_id] = session
        return session
    
    def get_sso_session(self, session_id: str) -> Optional[dict]:
        session = self.sso_sessions.get(session_id)
        if session and session["expires_at"] > datetime.utcnow().isoformat():
            return session
        return None


sso_store = SSOStore()


# =========================================================================
# SCIM Service
# =========================================================================

class SCIMService:
    """SCIM 2.0 user provisioning"""
    
    def __init__(self):
        self.users: Dict[str, dict] = {}
        self.groups: Dict[str, dict] = {}
        self._user_counter = 0
        self._group_counter = 0
    
    def create_user(self, data: dict) -> dict:
        self._user_counter += 1
        user_id = f"scim_user_{self._user_counter}"
        
        user = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "id": user_id,
            "meta": {
                "resourceType": "User",
                "created": datetime.utcnow().isoformat(),
                "lastModified": datetime.utcnow().isoformat()
            },
            **data
        }
        
        self.users[user_id] = user
        return user
    
    def get_user(self, user_id: str) -> Optional[dict]:
        return self.users.get(user_id)
    
    def list_users(self, filter_str: str = None, start: int = 1, count: int = 100) -> dict:
        users = list(self.users.values())
        
        # Apply filter (simplified)
        if filter_str and "userName eq" in filter_str:
            username = filter_str.split('"')[1] if '"' in filter_str else None
            if username:
                users = [u for u in users if u.get("userName") == username]
        
        total = len(users)
        users = users[start-1:start-1+count]
        
        return {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
            "totalResults": total,
            "startIndex": start,
            "itemsPerPage": len(users),
            "Resources": users
        }
    
    def update_user(self, user_id: str, data: dict) -> Optional[dict]:
        if user_id not in self.users:
            return None
        
        self.users[user_id].update(data)
        self.users[user_id]["meta"]["lastModified"] = datetime.utcnow().isoformat()
        return self.users[user_id]
    
    def delete_user(self, user_id: str) -> bool:
        if user_id in self.users:
            del self.users[user_id]
            return True
        return False
    
    def create_group(self, data: dict) -> dict:
        self._group_counter += 1
        group_id = f"scim_group_{self._group_counter}"
        
        group = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "id": group_id,
            "meta": {
                "resourceType": "Group",
                "created": datetime.utcnow().isoformat(),
                "lastModified": datetime.utcnow().isoformat()
            },
            **data
        }
        
        self.groups[group_id] = group
        return group
    
    def list_groups(self) -> dict:
        return {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
            "totalResults": len(self.groups),
            "Resources": list(self.groups.values())
        }


scim_service = SCIMService()


# =========================================================================
# Endpoints - SSO Configuration
# =========================================================================

@router.post("/config")
async def create_sso_config(config: SSOConfig):
    """Create SSO configuration"""
    result = sso_store.create_config(config.dict())
    return {"success": True, "config": result}


@router.get("/configs")
async def list_sso_configs(org_id: Optional[str] = Query(default=None)):
    """List SSO configurations"""
    return {"configs": sso_store.list_configs(org_id)}


@router.get("/config/{config_id}")
async def get_sso_config(config_id: str):
    """Get SSO configuration"""
    config = sso_store.get_config(config_id)
    if not config:
        raise HTTPException(status_code=404, detail="SSO config not found")
    return config


@router.put("/config/{config_id}")
async def update_sso_config(config_id: str, updates: dict):
    """Update SSO configuration"""
    result = sso_store.update_config(config_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="SSO config not found")
    return {"success": True, "config": result}


@router.delete("/config/{config_id}")
async def delete_sso_config(config_id: str):
    """Delete SSO configuration"""
    if not sso_store.delete_config(config_id):
        raise HTTPException(status_code=404, detail="SSO config not found")
    return {"success": True}


# =========================================================================
# SAML Endpoints
# =========================================================================

@router.get("/saml/{config_id}/metadata")
async def get_saml_metadata(config_id: str):
    """Get SAML SP metadata"""
    config = sso_store.get_config(config_id)
    if not config:
        raise HTTPException(status_code=404, detail="SSO config not found")
    
    # Generate SAML metadata XML
    metadata = f"""<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="{config['sp_entity_id']}">
  <md:SPSSODescriptor AuthnRequestsSigned="false" WantAssertionsSigned="true" protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</md:NameIDFormat>
    <md:AssertionConsumerService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://app.aca-datahub.com{config['acs_url']}" index="0"/>
  </md:SPSSODescriptor>
</md:EntityDescriptor>"""
    
    return Response(content=metadata, media_type="application/xml")


@router.get("/saml/{config_id}/login")
async def initiate_saml_login(config_id: str, request: Request):
    """Initiate SAML login"""
    config = sso_store.get_config(config_id)
    if not config:
        raise HTTPException(status_code=404, detail="SSO config not found")
    
    # Generate SAML AuthnRequest (simplified)
    request_id = f"_{''.join(secrets.token_hex(16))}"
    
    return {
        "redirect_url": config.get("sso_url"),
        "saml_request_id": request_id,
        "message": "Redirect to IdP for authentication"
    }


@router.post("/saml/{config_id}/acs")
async def saml_acs(config_id: str, request: Request):
    """SAML Assertion Consumer Service"""
    config = sso_store.get_config(config_id)
    if not config:
        raise HTTPException(status_code=404, detail="SSO config not found")
    
    # Parse SAML response (simplified - would validate signature in production)
    form_data = await request.form()
    saml_response = form_data.get("SAMLResponse")
    
    if not saml_response:
        raise HTTPException(status_code=400, detail="Missing SAML response")
    
    # Simulate successful SSO
    session = sso_store.create_sso_session(
        "user_sso_1",
        "saml",
        {"email": "user@company.com", "groups": ["admin"]}
    )
    
    return {
        "success": True,
        "session_id": session["id"],
        "redirect_url": "/dashboard"
    }


# =========================================================================
# OIDC Endpoints
# =========================================================================

@router.get("/oidc/{config_id}/authorize")
async def oidc_authorize(config_id: str, redirect_uri: str, state: str):
    """Initiate OIDC authorization"""
    config = sso_store.get_config(config_id)
    if not config:
        raise HTTPException(status_code=404, detail="SSO config not found")
    
    # Build OIDC authorization URL
    auth_url = f"{config.get('issuer_url')}/authorize"
    params = {
        "client_id": config.get("client_id"),
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state
    }
    
    return {
        "authorization_url": auth_url,
        "params": params
    }


@router.post("/oidc/{config_id}/callback")
async def oidc_callback(config_id: str, code: str, state: str):
    """OIDC authorization callback"""
    config = sso_store.get_config(config_id)
    if not config:
        raise HTTPException(status_code=404, detail="SSO config not found")
    
    # Exchange code for tokens (simplified)
    session = sso_store.create_sso_session(
        "user_oidc_1",
        "oidc",
        {"email": "user@company.com", "sub": "12345"}
    )
    
    return {
        "success": True,
        "session_id": session["id"],
        "id_token": "eyJ...",  # Would be real JWT
        "access_token": "eyJ..."
    }


# =========================================================================
# SCIM Endpoints
# =========================================================================

@router.get("/scim/v2/Users")
async def scim_list_users(
    filter: Optional[str] = Query(default=None),
    startIndex: int = Query(default=1),
    count: int = Query(default=100)
):
    """SCIM - List users"""
    return scim_service.list_users(filter, startIndex, count)


@router.post("/scim/v2/Users")
async def scim_create_user(user: SCIMUser):
    """SCIM - Create user"""
    result = scim_service.create_user(user.dict())
    return result


@router.get("/scim/v2/Users/{user_id}")
async def scim_get_user(user_id: str):
    """SCIM - Get user"""
    user = scim_service.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.put("/scim/v2/Users/{user_id}")
async def scim_update_user(user_id: str, user: SCIMUser):
    """SCIM - Update user"""
    result = scim_service.update_user(user_id, user.dict())
    if not result:
        raise HTTPException(status_code=404, detail="User not found")
    return result


@router.delete("/scim/v2/Users/{user_id}")
async def scim_delete_user(user_id: str):
    """SCIM - Delete user"""
    if not scim_service.delete_user(user_id):
        raise HTTPException(status_code=404, detail="User not found")
    return Response(status_code=204)


@router.get("/scim/v2/Groups")
async def scim_list_groups():
    """SCIM - List groups"""
    return scim_service.list_groups()


# =========================================================================
# IP Allowlisting
# =========================================================================

@router.post("/ip-allowlist")
async def add_ip_to_allowlist(org_id: str, ip_addresses: List[str]):
    """Add IPs to organization allowlist"""
    if org_id not in sso_store.ip_allowlist:
        sso_store.ip_allowlist[org_id] = []
    
    sso_store.ip_allowlist[org_id].extend(ip_addresses)
    sso_store.ip_allowlist[org_id] = list(set(sso_store.ip_allowlist[org_id]))
    
    return {"success": True, "allowlist": sso_store.ip_allowlist[org_id]}


@router.get("/ip-allowlist/{org_id}")
async def get_ip_allowlist(org_id: str):
    """Get organization IP allowlist"""
    return {"org_id": org_id, "allowlist": sso_store.ip_allowlist.get(org_id, [])}


@router.post("/ip-allowlist/{org_id}/verify")
async def verify_ip(org_id: str, ip_address: str):
    """Verify if IP is allowed"""
    allowlist = sso_store.ip_allowlist.get(org_id, [])
    is_allowed = not allowlist or ip_address in allowlist
    
    return {"ip": ip_address, "allowed": is_allowed}
