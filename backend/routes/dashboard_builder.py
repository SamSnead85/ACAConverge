"""
Dashboard Builder Routes for ACA DataHub
Create, manage, and share custom dashboards with widgets
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

router = APIRouter(prefix="/dashboards", tags=["Dashboards"])


# =========================================================================
# Models
# =========================================================================

class WidgetType(str, Enum):
    STAT_CARD = "stat_card"
    BAR_CHART = "bar_chart"
    LINE_CHART = "line_chart"
    PIE_CHART = "pie_chart"
    TABLE = "table"
    MAP = "map"
    TEXT = "text"
    METRIC = "metric"
    FUNNEL = "funnel"
    HEATMAP = "heatmap"


class WidgetConfig(BaseModel):
    id: Optional[str] = None
    type: WidgetType
    title: str
    x: int = 0
    y: int = 0
    width: int = 4
    height: int = 3
    config: Dict[str, Any] = {}
    query: Optional[str] = None
    refresh_interval: int = 0  # seconds, 0 = no auto-refresh


class DashboardCreate(BaseModel):
    name: str
    description: Optional[str] = None
    is_public: bool = False
    layout: str = "grid"
    widgets: List[WidgetConfig] = []


# =========================================================================
# Dashboard Store
# =========================================================================

class DashboardStore:
    """Stores dashboard configurations"""
    
    def __init__(self):
        self.dashboards: Dict[str, dict] = {}
        self._counter = 0
        self._widget_counter = 0
        
        # Initialize with sample dashboards
        self._init_samples()
    
    def _init_samples(self):
        """Create sample dashboards"""
        self.create({
            "name": "Lead Analytics",
            "description": "Overview of lead scoring and engagement",
            "is_public": True,
            "widgets": [
                {"type": "stat_card", "title": "Total Leads", "x": 0, "y": 0, "width": 3, "height": 2, "config": {"field": "count", "color": "primary"}},
                {"type": "stat_card", "title": "Avg Score", "x": 3, "y": 0, "width": 3, "height": 2, "config": {"field": "avg_score", "color": "success"}},
                {"type": "stat_card", "title": "Conversion Rate", "x": 6, "y": 0, "width": 3, "height": 2, "config": {"field": "conversion", "color": "warning"}},
                {"type": "stat_card", "title": "Email Ready", "x": 9, "y": 0, "width": 3, "height": 2, "config": {"field": "email_ready", "color": "info"}},
                {"type": "bar_chart", "title": "Leads by Score Range", "x": 0, "y": 2, "width": 6, "height": 4, "config": {"groupBy": "score_range"}},
                {"type": "pie_chart", "title": "Leads by Region", "x": 6, "y": 2, "width": 6, "height": 4, "config": {"groupBy": "region"}},
            ]
        })
        
        self.create({
            "name": "Campaign Performance",
            "description": "Track campaign metrics and ROI",
            "is_public": True,
            "widgets": [
                {"type": "line_chart", "title": "Sends Over Time", "x": 0, "y": 0, "width": 12, "height": 4, "config": {"metric": "sends", "period": "daily"}},
                {"type": "funnel", "title": "Conversion Funnel", "x": 0, "y": 4, "width": 6, "height": 4, "config": {"stages": ["sent", "opened", "clicked", "converted"]}},
                {"type": "table", "title": "Top Campaigns", "x": 6, "y": 4, "width": 6, "height": 4, "config": {"columns": ["name", "sent", "opened", "roi"]}},
            ]
        })
    
    def create(self, data: dict) -> dict:
        self._counter += 1
        dashboard_id = f"dash_{self._counter}"
        
        # Generate widget IDs
        widgets = data.get("widgets", [])
        for widget in widgets:
            if not widget.get("id"):
                self._widget_counter += 1
                widget["id"] = f"widget_{self._widget_counter}"
        
        dashboard = {
            "id": dashboard_id,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            **data
        }
        
        self.dashboards[dashboard_id] = dashboard
        return dashboard
    
    def get(self, dashboard_id: str) -> Optional[dict]:
        return self.dashboards.get(dashboard_id)
    
    def list(self, is_public: Optional[bool] = None) -> List[dict]:
        dashboards = list(self.dashboards.values())
        if is_public is not None:
            dashboards = [d for d in dashboards if d.get("is_public") == is_public]
        return sorted(dashboards, key=lambda x: x.get("updated_at", ""), reverse=True)
    
    def update(self, dashboard_id: str, updates: dict) -> Optional[dict]:
        if dashboard_id not in self.dashboards:
            return None
        updates["updated_at"] = datetime.utcnow().isoformat()
        self.dashboards[dashboard_id].update(updates)
        return self.dashboards[dashboard_id]
    
    def delete(self, dashboard_id: str) -> bool:
        if dashboard_id in self.dashboards:
            del self.dashboards[dashboard_id]
            return True
        return False
    
    def add_widget(self, dashboard_id: str, widget: dict) -> Optional[dict]:
        if dashboard_id not in self.dashboards:
            return None
        
        self._widget_counter += 1
        widget["id"] = f"widget_{self._widget_counter}"
        
        self.dashboards[dashboard_id]["widgets"].append(widget)
        self.dashboards[dashboard_id]["updated_at"] = datetime.utcnow().isoformat()
        
        return widget
    
    def update_widget(self, dashboard_id: str, widget_id: str, updates: dict) -> Optional[dict]:
        if dashboard_id not in self.dashboards:
            return None
        
        widgets = self.dashboards[dashboard_id].get("widgets", [])
        for widget in widgets:
            if widget.get("id") == widget_id:
                widget.update(updates)
                self.dashboards[dashboard_id]["updated_at"] = datetime.utcnow().isoformat()
                return widget
        
        return None
    
    def remove_widget(self, dashboard_id: str, widget_id: str) -> bool:
        if dashboard_id not in self.dashboards:
            return False
        
        widgets = self.dashboards[dashboard_id].get("widgets", [])
        original_len = len(widgets)
        self.dashboards[dashboard_id]["widgets"] = [w for w in widgets if w.get("id") != widget_id]
        
        if len(self.dashboards[dashboard_id]["widgets"]) < original_len:
            self.dashboards[dashboard_id]["updated_at"] = datetime.utcnow().isoformat()
            return True
        
        return False


dashboard_store = DashboardStore()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("", response_model=dict)
async def create_dashboard(dashboard: DashboardCreate):
    """Create a new dashboard"""
    result = dashboard_store.create(dashboard.dict())
    return {"success": True, "dashboard": result}


@router.get("")
async def list_dashboards(
    is_public: Optional[bool] = Query(default=None)
):
    """List all dashboards"""
    dashboards = dashboard_store.list(is_public)
    # Return without full widget details for list view
    return {
        "dashboards": [
            {
                "id": d["id"],
                "name": d["name"],
                "description": d.get("description"),
                "is_public": d.get("is_public", False),
                "widget_count": len(d.get("widgets", [])),
                "updated_at": d.get("updated_at")
            }
            for d in dashboards
        ]
    }


@router.get("/templates")
async def get_dashboard_templates():
    """Get pre-built dashboard templates"""
    templates = [
        {
            "id": "tpl_lead_analytics",
            "name": "Lead Analytics",
            "description": "Comprehensive lead scoring and segmentation dashboard",
            "preview_image": "/templates/lead_analytics.png",
            "widget_count": 6,
            "category": "Analytics"
        },
        {
            "id": "tpl_campaign_perf",
            "name": "Campaign Performance",
            "description": "Track email campaign metrics and ROI",
            "preview_image": "/templates/campaign_perf.png",
            "widget_count": 5,
            "category": "Marketing"
        },
        {
            "id": "tpl_executive",
            "name": "Executive Summary",
            "description": "High-level KPIs and trend overview",
            "preview_image": "/templates/executive.png",
            "widget_count": 8,
            "category": "Executive"
        },
        {
            "id": "tpl_data_quality",
            "name": "Data Quality Monitor",
            "description": "Track data completeness and quality scores",
            "preview_image": "/templates/data_quality.png",
            "widget_count": 4,
            "category": "Operations"
        }
    ]
    return {"templates": templates}


@router.get("/{dashboard_id}")
async def get_dashboard(dashboard_id: str):
    """Get a specific dashboard with all widgets"""
    dashboard = dashboard_store.get(dashboard_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return dashboard


@router.put("/{dashboard_id}")
async def update_dashboard(dashboard_id: str, updates: dict):
    """Update a dashboard"""
    result = dashboard_store.update(dashboard_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return {"success": True, "dashboard": result}


@router.delete("/{dashboard_id}")
async def delete_dashboard(dashboard_id: str):
    """Delete a dashboard"""
    if not dashboard_store.delete(dashboard_id):
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return {"success": True}


# =========================================================================
# Widget Management
# =========================================================================

@router.post("/{dashboard_id}/widgets")
async def add_widget(dashboard_id: str, widget: WidgetConfig):
    """Add a widget to a dashboard"""
    result = dashboard_store.add_widget(dashboard_id, widget.dict())
    if not result:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return {"success": True, "widget": result}


@router.put("/{dashboard_id}/widgets/{widget_id}")
async def update_widget(dashboard_id: str, widget_id: str, updates: dict):
    """Update a widget in a dashboard"""
    result = dashboard_store.update_widget(dashboard_id, widget_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Dashboard or widget not found")
    return {"success": True, "widget": result}


@router.delete("/{dashboard_id}/widgets/{widget_id}")
async def remove_widget(dashboard_id: str, widget_id: str):
    """Remove a widget from a dashboard"""
    if not dashboard_store.remove_widget(dashboard_id, widget_id):
        raise HTTPException(status_code=404, detail="Dashboard or widget not found")
    return {"success": True}


# =========================================================================
# Layout Management
# =========================================================================

@router.put("/{dashboard_id}/layout")
async def update_dashboard_layout(
    dashboard_id: str,
    widgets: List[dict]
):
    """Update the layout (positions/sizes) of all widgets"""
    dashboard = dashboard_store.get(dashboard_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    
    # Update positions for each widget
    for widget_update in widgets:
        widget_id = widget_update.get("id")
        if widget_id:
            dashboard_store.update_widget(dashboard_id, widget_id, {
                "x": widget_update.get("x"),
                "y": widget_update.get("y"),
                "width": widget_update.get("width"),
                "height": widget_update.get("height")
            })
    
    return {"success": True}


# =========================================================================
# Dashboard Sharing
# =========================================================================

@router.post("/{dashboard_id}/share")
async def share_dashboard(
    dashboard_id: str,
    is_public: bool = True,
    embed_enabled: bool = False
):
    """Configure sharing settings for a dashboard"""
    dashboard = dashboard_store.get(dashboard_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    
    share_id = f"share_{dashboard_id}"
    
    dashboard_store.update(dashboard_id, {
        "is_public": is_public,
        "embed_enabled": embed_enabled,
        "share_id": share_id
    })
    
    return {
        "success": True,
        "share_url": f"/shared/{share_id}" if is_public else None,
        "embed_code": f'<iframe src="/embed/{share_id}" width="800" height="600"></iframe>' if embed_enabled else None
    }


@router.get("/shared/{share_id}")
async def get_shared_dashboard(share_id: str):
    """Get a shared dashboard by share ID"""
    for dashboard in dashboard_store.dashboards.values():
        if dashboard.get("share_id") == share_id and dashboard.get("is_public"):
            return dashboard
    
    raise HTTPException(status_code=404, detail="Dashboard not found or not public")
