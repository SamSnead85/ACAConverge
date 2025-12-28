"""
White-Label & Customization Routes for ACA DataHub
Theme builder, branding, partner portal, and multi-brand management
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

router = APIRouter(prefix="/white-label", tags=["White-Label"])


# =========================================================================
# Models
# =========================================================================

class ThemeConfig(BaseModel):
    primary_color: str = "#6366f1"
    secondary_color: str = "#22d3ee"
    accent_color: str = "#f59e0b"
    background_color: str = "#0f0f1a"
    surface_color: str = "#1a1a2e"
    text_color: str = "#ffffff"
    font_family: str = "Inter, sans-serif"
    border_radius: str = "12px"


class BrandConfig(BaseModel):
    company_name: str
    logo_url: Optional[str] = None
    favicon_url: Optional[str] = None
    support_email: Optional[str] = None
    support_url: Optional[str] = None
    custom_domain: Optional[str] = None
    theme: Optional[ThemeConfig] = None


# =========================================================================
# White-Label Store
# =========================================================================

class WhiteLabelStore:
    """Manages white-label configurations per tenant"""
    
    def __init__(self):
        self.brands: Dict[str, dict] = {}
        self.themes: Dict[str, dict] = {}
        self.feature_flags: Dict[str, Dict[str, bool]] = {}
        self.email_templates: Dict[str, Dict[str, dict]] = {}
        self._counter = 0
    
    def create_brand(self, org_id: str, config: dict) -> dict:
        brand = {
            "id": org_id,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            **config
        }
        
        self.brands[org_id] = brand
        
        # Initialize default feature flags
        self.feature_flags[org_id] = {
            "ai_assistant": True,
            "campaigns": True,
            "integrations": True,
            "marketplace": False,
            "advanced_analytics": True,
            "export_pdf": True,
            "sso": False,
            "api_access": True
        }
        
        return brand
    
    def get_brand(self, org_id: str) -> Optional[dict]:
        return self.brands.get(org_id)
    
    def update_brand(self, org_id: str, updates: dict) -> Optional[dict]:
        if org_id not in self.brands:
            return None
        
        updates["updated_at"] = datetime.utcnow().isoformat()
        self.brands[org_id].update(updates)
        return self.brands[org_id]
    
    def create_theme(self, org_id: str, theme: dict) -> dict:
        self.themes[org_id] = {
            "org_id": org_id,
            "created_at": datetime.utcnow().isoformat(),
            **theme
        }
        return self.themes[org_id]
    
    def get_theme(self, org_id: str) -> dict:
        if org_id in self.themes:
            return self.themes[org_id]
        
        # Return default theme
        return {
            "primary_color": "#6366f1",
            "secondary_color": "#22d3ee",
            "accent_color": "#f59e0b",
            "background_color": "#0f0f1a",
            "surface_color": "#1a1a2e",
            "text_color": "#ffffff",
            "font_family": "Inter, sans-serif",
            "border_radius": "12px"
        }
    
    def set_feature_flags(self, org_id: str, flags: Dict[str, bool]) -> dict:
        if org_id not in self.feature_flags:
            self.feature_flags[org_id] = {}
        
        self.feature_flags[org_id].update(flags)
        return self.feature_flags[org_id]
    
    def get_feature_flags(self, org_id: str) -> Dict[str, bool]:
        return self.feature_flags.get(org_id, {})
    
    def set_email_template(self, org_id: str, template_type: str, template: dict) -> dict:
        if org_id not in self.email_templates:
            self.email_templates[org_id] = {}
        
        self.email_templates[org_id][template_type] = {
            "type": template_type,
            "updated_at": datetime.utcnow().isoformat(),
            **template
        }
        return self.email_templates[org_id][template_type]
    
    def get_email_templates(self, org_id: str) -> Dict[str, dict]:
        return self.email_templates.get(org_id, {})
    
    def generate_css(self, org_id: str) -> str:
        theme = self.get_theme(org_id)
        
        css = f"""
:root {{
    --primary-color: {theme.get('primary_color', '#6366f1')};
    --secondary-color: {theme.get('secondary_color', '#22d3ee')};
    --accent-color: {theme.get('accent_color', '#f59e0b')};
    --background-color: {theme.get('background_color', '#0f0f1a')};
    --surface-color: {theme.get('surface_color', '#1a1a2e')};
    --text-color: {theme.get('text_color', '#ffffff')};
    --font-family: {theme.get('font_family', 'Inter, sans-serif')};
    --border-radius: {theme.get('border_radius', '12px')};
}}

body {{
    background-color: var(--background-color);
    color: var(--text-color);
    font-family: var(--font-family);
}}

.btn-primary {{
    background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
    border-radius: var(--border-radius);
}}

.card {{
    background: var(--surface-color);
    border-radius: var(--border-radius);
}}
"""
        return css


white_label_store = WhiteLabelStore()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/brands")
async def create_brand(
    org_id: str = Query(...),
    company_name: str = Query(...),
    logo_url: Optional[str] = Query(default=None),
    custom_domain: Optional[str] = Query(default=None)
):
    """Create brand configuration"""
    brand = white_label_store.create_brand(org_id, {
        "company_name": company_name,
        "logo_url": logo_url,
        "custom_domain": custom_domain
    })
    return {"success": True, "brand": brand}


@router.get("/brands/{org_id}")
async def get_brand(org_id: str):
    """Get brand configuration"""
    brand = white_label_store.get_brand(org_id)
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")
    return brand


@router.put("/brands/{org_id}")
async def update_brand(org_id: str, updates: dict):
    """Update brand configuration"""
    result = white_label_store.update_brand(org_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Brand not found")
    return {"success": True, "brand": result}


@router.post("/themes/{org_id}")
async def create_theme(org_id: str, theme: ThemeConfig):
    """Create or update theme"""
    result = white_label_store.create_theme(org_id, theme.dict())
    return {"success": True, "theme": result}


@router.get("/themes/{org_id}")
async def get_theme(org_id: str):
    """Get theme configuration"""
    return white_label_store.get_theme(org_id)


@router.get("/themes/{org_id}/css")
async def get_theme_css(org_id: str):
    """Get generated CSS for theme"""
    from fastapi.responses import PlainTextResponse
    css = white_label_store.generate_css(org_id)
    return PlainTextResponse(content=css, media_type="text/css")


@router.put("/feature-flags/{org_id}")
async def set_feature_flags(org_id: str, flags: Dict[str, bool]):
    """Set feature flags for organization"""
    result = white_label_store.set_feature_flags(org_id, flags)
    return {"success": True, "flags": result}


@router.get("/feature-flags/{org_id}")
async def get_feature_flags(org_id: str):
    """Get feature flags for organization"""
    return {"org_id": org_id, "flags": white_label_store.get_feature_flags(org_id)}


@router.post("/email-templates/{org_id}/{template_type}")
async def set_email_template(
    org_id: str,
    template_type: str,
    subject: str = Query(...),
    html_body: str = Query(...),
    from_name: Optional[str] = Query(default=None)
):
    """Set branded email template"""
    result = white_label_store.set_email_template(org_id, template_type, {
        "subject": subject,
        "html_body": html_body,
        "from_name": from_name
    })
    return {"success": True, "template": result}


@router.get("/email-templates/{org_id}")
async def get_email_templates(org_id: str):
    """Get all email templates for organization"""
    return {"templates": white_label_store.get_email_templates(org_id)}


@router.get("/login-page/{org_id}")
async def get_login_page_config(org_id: str):
    """Get login page customization"""
    brand = white_label_store.get_brand(org_id) or {}
    theme = white_label_store.get_theme(org_id)
    
    return {
        "org_id": org_id,
        "company_name": brand.get("company_name", "ACA DataHub"),
        "logo_url": brand.get("logo_url"),
        "background_image": brand.get("login_background"),
        "theme": theme,
        "show_powered_by": brand.get("show_powered_by", True)
    }


@router.get("/reseller-dashboard/{org_id}")
async def get_reseller_dashboard(org_id: str):
    """Get reseller/partner dashboard data"""
    brand = white_label_store.get_brand(org_id)
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")
    
    # Simulate sub-tenant data
    return {
        "org_id": org_id,
        "company_name": brand.get("company_name"),
        "sub_tenants": [
            {"id": "sub_1", "name": "Client A", "users": 5, "status": "active"},
            {"id": "sub_2", "name": "Client B", "users": 12, "status": "active"},
            {"id": "sub_3", "name": "Client C", "users": 3, "status": "trial"}
        ],
        "total_mrr": 2500,
        "commission_rate": 0.20,
        "pending_commission": 500
    }
