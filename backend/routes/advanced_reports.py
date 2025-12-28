"""
Advanced Reporting Routes for ACA DataHub
Scheduled reports, subscriptions, PDF generation, and embedded analytics
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum

router = APIRouter(prefix="/reports", tags=["Advanced Reports"])


# =========================================================================
# Models
# =========================================================================

class ReportFormat(str, Enum):
    PDF = "pdf"
    EXCEL = "excel"
    CSV = "csv"
    JSON = "json"
    HTML = "html"


class DeliveryChannel(str, Enum):
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    DOWNLOAD = "download"


class ScheduleFrequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


# =========================================================================
# Report Store
# =========================================================================

class ReportStore:
    """Manages report templates, schedules, and deliveries"""
    
    def __init__(self):
        self.templates: Dict[str, dict] = {}
        self.schedules: Dict[str, dict] = {}
        self.subscriptions: Dict[str, dict] = {}
        self.reports: Dict[str, dict] = {}
        self._counter = 0
        self._init_templates()
    
    def _init_templates(self):
        """Initialize built-in report templates"""
        self.templates = {
            "tpl_lead_summary": {
                "id": "tpl_lead_summary",
                "name": "Lead Summary Report",
                "description": "Overview of lead metrics and scoring",
                "category": "Marketing",
                "parameters": [
                    {"name": "date_range", "type": "date_range", "required": True},
                    {"name": "score_threshold", "type": "number", "default": 0}
                ],
                "sections": ["overview", "score_distribution", "top_leads", "trends"]
            },
            "tpl_campaign_perf": {
                "id": "tpl_campaign_perf",
                "name": "Campaign Performance Report",
                "description": "Detailed campaign analytics and ROI",
                "category": "Marketing",
                "parameters": [
                    {"name": "campaign_ids", "type": "array", "required": False},
                    {"name": "date_range", "type": "date_range", "required": True}
                ],
                "sections": ["summary", "channel_breakdown", "conversion_funnel", "ab_results"]
            },
            "tpl_executive": {
                "id": "tpl_executive",
                "name": "Executive Dashboard Report",
                "description": "High-level KPIs for leadership",
                "category": "Executive",
                "parameters": [
                    {"name": "period", "type": "string", "default": "monthly"}
                ],
                "sections": ["kpis", "trends", "highlights", "forecasts"]
            },
            "tpl_compliance": {
                "id": "tpl_compliance",
                "name": "Compliance Audit Report",
                "description": "Data access and compliance summary",
                "category": "Compliance",
                "parameters": [
                    {"name": "date_range", "type": "date_range", "required": True}
                ],
                "sections": ["access_log", "pii_summary", "data_requests", "policy_adherence"]
            }
        }
    
    def create_report(self, template_id: str, parameters: dict, user_id: str) -> dict:
        self._counter += 1
        report_id = f"rpt_{self._counter}"
        
        template = self.templates.get(template_id)
        if not template:
            raise ValueError("Template not found")
        
        report = {
            "id": report_id,
            "template_id": template_id,
            "template_name": template["name"],
            "parameters": parameters,
            "user_id": user_id,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "download_url": None,
            "version": 1
        }
        
        self.reports[report_id] = report
        return report
    
    def generate_report(self, report_id: str, format: str = "pdf") -> dict:
        if report_id not in self.reports:
            raise ValueError("Report not found")
        
        report = self.reports[report_id]
        report["status"] = "completed"
        report["completed_at"] = datetime.utcnow().isoformat()
        report["format"] = format
        report["download_url"] = f"/api/reports/{report_id}/download"
        report["file_size"] = "2.3 MB"
        
        return report
    
    def get_report(self, report_id: str) -> Optional[dict]:
        return self.reports.get(report_id)
    
    def list_reports(self, user_id: str = None) -> List[dict]:
        reports = list(self.reports.values())
        if user_id:
            reports = [r for r in reports if r.get("user_id") == user_id]
        return sorted(reports, key=lambda x: x.get("created_at", ""), reverse=True)
    
    def create_schedule(self, data: dict) -> dict:
        self._counter += 1
        schedule_id = f"sched_{self._counter}"
        
        schedule = {
            "id": schedule_id,
            "status": "active",
            "created_at": datetime.utcnow().isoformat(),
            "last_run": None,
            "next_run": self._calculate_next_run(data.get("frequency")),
            **data
        }
        
        self.schedules[schedule_id] = schedule
        return schedule
    
    def _calculate_next_run(self, frequency: str) -> str:
        now = datetime.utcnow()
        if frequency == "daily":
            next_run = now + timedelta(days=1)
        elif frequency == "weekly":
            next_run = now + timedelta(weeks=1)
        elif frequency == "monthly":
            next_run = now + timedelta(days=30)
        else:
            next_run = now + timedelta(days=90)
        return next_run.isoformat()
    
    def subscribe(self, report_id: str, user_id: str, channel: str, settings: dict = None) -> dict:
        self._counter += 1
        sub_id = f"rsub_{self._counter}"
        
        subscription = {
            "id": sub_id,
            "report_id": report_id,
            "user_id": user_id,
            "channel": channel,
            "settings": settings or {},
            "active": True,
            "created_at": datetime.utcnow().isoformat()
        }
        
        self.subscriptions[sub_id] = subscription
        return subscription
    
    def list_subscriptions(self, user_id: str) -> List[dict]:
        return [s for s in self.subscriptions.values() 
                if s.get("user_id") == user_id and s.get("active")]


report_store = ReportStore()


# =========================================================================
# PDF Generator (Simplified)
# =========================================================================

class PDFGenerator:
    """Generates PDF reports"""
    
    def generate(self, report: dict) -> bytes:
        """Generate PDF content (would use reportlab or weasyprint in production)"""
        template_name = report.get("template_name", "Report")
        
        # HTML template for PDF
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; padding: 40px; }}
                h1 {{ color: #333; border-bottom: 2px solid #6366f1; }}
                .header {{ display: flex; justify-content: space-between; }}
                .metric {{ background: #f0f0f0; padding: 20px; margin: 10px 0; }}
                .metric-value {{ font-size: 2em; font-weight: bold; color: #6366f1; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{template_name}</h1>
                <p>Generated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M")}</p>
            </div>
            
            <div class="metric">
                <div class="metric-value">2,543</div>
                <div>Total Leads</div>
            </div>
            
            <div class="metric">
                <div class="metric-value">3.2%</div>
                <div>Conversion Rate</div>
            </div>
            
            <h2>Summary</h2>
            <p>This report provides an overview of key metrics and trends.</p>
            
            <h2>Recommendations</h2>
            <ul>
                <li>Focus on high-score leads for improved conversion</li>
                <li>Optimize email send times based on engagement data</li>
                <li>Expand Georgia campaigns based on regional performance</li>
            </ul>
        </body>
        </html>
        """
        
        return html_content.encode()


pdf_generator = PDFGenerator()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/templates")
async def list_report_templates(category: Optional[str] = Query(default=None)):
    """List available report templates"""
    templates = list(report_store.templates.values())
    if category:
        templates = [t for t in templates if t.get("category") == category]
    return {"templates": templates}


@router.get("/templates/{template_id}")
async def get_report_template(template_id: str):
    """Get report template details"""
    template = report_store.templates.get(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    return template


@router.post("")
async def create_report(
    template_id: str = Query(...),
    parameters: dict = None,
    user_id: str = Query(...)
):
    """Create a new report"""
    try:
        report = report_store.create_report(template_id, parameters or {}, user_id)
        return {"success": True, "report": report}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{report_id}/generate")
async def generate_report(
    report_id: str,
    format: ReportFormat = Query(default=ReportFormat.PDF),
    background_tasks: BackgroundTasks = None
):
    """Generate report in specified format"""
    try:
        report = report_store.generate_report(report_id, format.value)
        return {"success": True, "report": report}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{report_id}")
async def get_report(report_id: str):
    """Get report details"""
    report = report_store.get_report(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    return report


@router.get("")
async def list_reports(user_id: Optional[str] = Query(default=None)):
    """List generated reports"""
    return {"reports": report_store.list_reports(user_id)}


@router.get("/{report_id}/download")
async def download_report(report_id: str):
    """Download generated report"""
    report = report_store.get_report(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    if report["status"] != "completed":
        raise HTTPException(status_code=400, detail="Report not ready")
    
    # In production, would return actual file
    return {
        "download_url": f"https://storage.aca-datahub.com/reports/{report_id}.pdf",
        "expires_at": (datetime.utcnow() + timedelta(hours=24)).isoformat()
    }


@router.post("/schedule")
async def create_report_schedule(
    template_id: str = Query(...),
    frequency: ScheduleFrequency = Query(...),
    parameters: dict = None,
    recipients: List[str] = Query(default=[]),
    channel: DeliveryChannel = Query(default=DeliveryChannel.EMAIL)
):
    """Create scheduled report delivery"""
    schedule = report_store.create_schedule({
        "template_id": template_id,
        "frequency": frequency.value,
        "parameters": parameters or {},
        "recipients": recipients,
        "channel": channel.value
    })
    return {"success": True, "schedule": schedule}


@router.get("/schedules")
async def list_schedules():
    """List report schedules"""
    return {"schedules": list(report_store.schedules.values())}


@router.delete("/schedule/{schedule_id}")
async def delete_schedule(schedule_id: str):
    """Delete report schedule"""
    if schedule_id in report_store.schedules:
        del report_store.schedules[schedule_id]
        return {"success": True}
    raise HTTPException(status_code=404, detail="Schedule not found")


@router.post("/{report_id}/subscribe")
async def subscribe_to_report(
    report_id: str,
    user_id: str = Query(...),
    channel: DeliveryChannel = Query(default=DeliveryChannel.EMAIL),
    settings: dict = None
):
    """Subscribe to report updates"""
    subscription = report_store.subscribe(report_id, user_id, channel.value, settings)
    return {"success": True, "subscription": subscription}


@router.get("/subscriptions")
async def list_subscriptions(user_id: str = Query(...)):
    """List user's report subscriptions"""
    return {"subscriptions": report_store.list_subscriptions(user_id)}


@router.get("/embed/{report_id}")
async def get_embed_config(report_id: str):
    """Get embed configuration for analytics SDK"""
    report = report_store.get_report(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    # Generate embed token
    embed_token = f"embed_{''.join([str(hash(report_id))[:20]])}"
    
    return {
        "embed_token": embed_token,
        "embed_url": f"https://embed.aca-datahub.com/v1/{report_id}",
        "expires_at": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
        "options": {
            "theme": "light",
            "responsive": True,
            "show_toolbar": True
        }
    }
