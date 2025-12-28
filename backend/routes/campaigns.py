"""
Campaign & Marketing Suite Routes for ACA DataHub
Multi-channel campaigns, A/B testing, and performance analytics
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import string

router = APIRouter(prefix="/campaigns", tags=["Campaigns"])


# =========================================================================
# Models
# =========================================================================

class CampaignStatus(str, Enum):
    DRAFT = "draft"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class CampaignChannel(str, Enum):
    EMAIL = "email"
    SMS = "sms"
    BOTH = "both"


class CampaignCreate(BaseModel):
    name: str
    description: Optional[str] = None
    channel: CampaignChannel
    population_ids: List[str]
    template_id: Optional[str] = None
    subject: Optional[str] = None  # For email
    content: str
    schedule_at: Optional[str] = None
    ab_test_enabled: bool = False
    ab_variants: Optional[List[dict]] = None


class ABVariant(BaseModel):
    name: str
    subject: Optional[str] = None
    content: str
    percentage: float = 50.0


# =========================================================================
# Campaign Store
# =========================================================================

class CampaignStore:
    """Stores campaign data and analytics"""
    
    def __init__(self):
        self.campaigns: Dict[str, dict] = {}
        self.analytics: Dict[str, dict] = {}
        self._counter = 0
    
    def create(self, data: dict) -> dict:
        self._counter += 1
        campaign_id = f"camp_{self._counter}"
        
        campaign = {
            "id": campaign_id,
            "status": CampaignStatus.DRAFT.value,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "launched_at": None,
            "completed_at": None,
            **data
        }
        
        self.campaigns[campaign_id] = campaign
        
        # Initialize analytics
        self.analytics[campaign_id] = {
            "sent": 0,
            "delivered": 0,
            "opened": 0,
            "clicked": 0,
            "bounced": 0,
            "unsubscribed": 0,
            "converted": 0,
            "ab_results": {}
        }
        
        return campaign
    
    def get(self, campaign_id: str) -> Optional[dict]:
        return self.campaigns.get(campaign_id)
    
    def list(self, status: Optional[str] = None) -> List[dict]:
        campaigns = list(self.campaigns.values())
        if status:
            campaigns = [c for c in campaigns if c.get("status") == status]
        return sorted(campaigns, key=lambda x: x.get("created_at", ""), reverse=True)
    
    def update(self, campaign_id: str, updates: dict) -> Optional[dict]:
        if campaign_id not in self.campaigns:
            return None
        updates["updated_at"] = datetime.utcnow().isoformat()
        self.campaigns[campaign_id].update(updates)
        return self.campaigns[campaign_id]
    
    def delete(self, campaign_id: str) -> bool:
        if campaign_id in self.campaigns:
            del self.campaigns[campaign_id]
            if campaign_id in self.analytics:
                del self.analytics[campaign_id]
            return True
        return False
    
    def record_event(self, campaign_id: str, event: str, variant: str = None):
        """Record a campaign event (open, click, etc.)"""
        if campaign_id not in self.analytics:
            return
        
        if event in self.analytics[campaign_id]:
            self.analytics[campaign_id][event] += 1
        
        if variant and event in ["opened", "clicked", "converted"]:
            if variant not in self.analytics[campaign_id]["ab_results"]:
                self.analytics[campaign_id]["ab_results"][variant] = {
                    "sent": 0, "opened": 0, "clicked": 0, "converted": 0
                }
            self.analytics[campaign_id]["ab_results"][variant][event] += 1
    
    def get_analytics(self, campaign_id: str) -> Optional[dict]:
        analytics = self.analytics.get(campaign_id)
        if not analytics:
            return None
        
        # Calculate rates
        sent = max(1, analytics["sent"])
        analytics["open_rate"] = round(analytics["opened"] / sent * 100, 2)
        analytics["click_rate"] = round(analytics["clicked"] / sent * 100, 2)
        analytics["bounce_rate"] = round(analytics["bounced"] / sent * 100, 2)
        analytics["conversion_rate"] = round(analytics["converted"] / sent * 100, 2)
        
        return analytics


campaign_store = CampaignStore()


# =========================================================================
# Template Gallery
# =========================================================================

TEMPLATE_GALLERY = [
    {
        "id": "tpl_enrollment",
        "name": "ACA Enrollment Invitation",
        "category": "enrollment",
        "channel": "email",
        "subject": "You May Qualify for Affordable Health Coverage",
        "content": """Hello {{name}},

Great news! Based on your profile, you may qualify for affordable health insurance through the Affordable Care Act Marketplace.

**Why enroll?**
- Coverage for doctor visits, prescriptions, and emergencies
- Potential subsidies to lower your monthly premium
- Plans starting as low as $0/month

**Your personalized enrollment link:**
{{enrollment_link}}

Open enrollment ends soon - don't miss out on this opportunity to protect yourself and your family.

Questions? Reply to this email or call us at (800) 555-0123.

Best regards,
The ACA DataHub Team"""
    },
    {
        "id": "tpl_reminder",
        "name": "Enrollment Deadline Reminder",
        "category": "reminder",
        "channel": "email",
        "subject": "⏰ Only {{days_left}} Days Left to Enroll!",
        "content": """Hi {{name}},

Time is running out! Open enrollment ends in just {{days_left}} days.

Don't miss your chance to get covered for 2025.

👉 Complete your enrollment: {{enrollment_link}}

Need help? We're here for you.

Best,
ACA DataHub Team"""
    },
    {
        "id": "tpl_sms_quick",
        "name": "Quick SMS Outreach",
        "category": "outreach",
        "channel": "sms",
        "content": "Hi {{name}}! You may qualify for $0 health insurance. Get covered: {{enrollment_link}} Reply STOP to opt out."
    },
    {
        "id": "tpl_followup",
        "name": "Follow-Up Sequence",
        "category": "followup",
        "channel": "email",
        "subject": "Quick follow-up on your health coverage",
        "content": """Hi {{name}},

I wanted to follow up on your health insurance options. Have you had a chance to review the plans available to you?

I'm here to help answer any questions you might have.

{{enrollment_link}}

Best,
{{sender_name}}"""
    }
]


# =========================================================================
# Endpoints
# =========================================================================

@router.post("", response_model=dict)
async def create_campaign(campaign: CampaignCreate):
    """Create a new campaign"""
    result = campaign_store.create(campaign.dict())
    return {"success": True, "campaign": result}


@router.get("")
async def list_campaigns(
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=50, le=100)
):
    """List all campaigns"""
    campaigns = campaign_store.list(status)[:limit]
    return {"campaigns": campaigns, "total": len(campaigns)}


@router.get("/templates")
async def get_template_gallery(
    category: Optional[str] = Query(default=None),
    channel: Optional[str] = Query(default=None)
):
    """Get pre-built campaign templates"""
    templates = TEMPLATE_GALLERY
    
    if category:
        templates = [t for t in templates if t.get("category") == category]
    if channel:
        templates = [t for t in templates if t.get("channel") == channel]
    
    return {"templates": templates}


@router.get("/{campaign_id}")
async def get_campaign(campaign_id: str):
    """Get a specific campaign"""
    campaign = campaign_store.get(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return campaign


@router.put("/{campaign_id}")
async def update_campaign(campaign_id: str, updates: dict):
    """Update a campaign"""
    result = campaign_store.update(campaign_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return {"success": True, "campaign": result}


@router.delete("/{campaign_id}")
async def delete_campaign(campaign_id: str):
    """Delete a campaign"""
    if not campaign_store.delete(campaign_id):
        raise HTTPException(status_code=404, detail="Campaign not found")
    return {"success": True}


@router.post("/{campaign_id}/launch")
async def launch_campaign(campaign_id: str, background_tasks: BackgroundTasks):
    """Launch a campaign"""
    campaign = campaign_store.get(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    if campaign["status"] not in [CampaignStatus.DRAFT.value, CampaignStatus.SCHEDULED.value]:
        raise HTTPException(status_code=400, detail="Campaign cannot be launched in current status")
    
    # Update status
    campaign_store.update(campaign_id, {
        "status": CampaignStatus.RUNNING.value,
        "launched_at": datetime.utcnow().isoformat()
    })
    
    # Simulate sending (in production would queue actual sends)
    sample_sent = random.randint(100, 1000)
    campaign_store.analytics[campaign_id]["sent"] = sample_sent
    campaign_store.analytics[campaign_id]["delivered"] = int(sample_sent * 0.95)
    
    return {"success": True, "message": "Campaign launched", "estimated_recipients": sample_sent}


@router.post("/{campaign_id}/pause")
async def pause_campaign(campaign_id: str):
    """Pause a running campaign"""
    campaign = campaign_store.get(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    if campaign["status"] != CampaignStatus.RUNNING.value:
        raise HTTPException(status_code=400, detail="Only running campaigns can be paused")
    
    campaign_store.update(campaign_id, {"status": CampaignStatus.PAUSED.value})
    return {"success": True}


@router.post("/{campaign_id}/resume")
async def resume_campaign(campaign_id: str):
    """Resume a paused campaign"""
    campaign = campaign_store.get(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    if campaign["status"] != CampaignStatus.PAUSED.value:
        raise HTTPException(status_code=400, detail="Only paused campaigns can be resumed")
    
    campaign_store.update(campaign_id, {"status": CampaignStatus.RUNNING.value})
    return {"success": True}


# =========================================================================
# A/B Testing
# =========================================================================

@router.post("/{campaign_id}/ab-test")
async def setup_ab_test(campaign_id: str, variants: List[ABVariant]):
    """Set up A/B testing for a campaign"""
    campaign = campaign_store.get(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    if campaign["status"] != CampaignStatus.DRAFT.value:
        raise HTTPException(status_code=400, detail="A/B test can only be set up for draft campaigns")
    
    # Validate percentages sum to 100
    total_pct = sum(v.percentage for v in variants)
    if abs(total_pct - 100) > 0.1:
        raise HTTPException(status_code=400, detail="Variant percentages must sum to 100")
    
    campaign_store.update(campaign_id, {
        "ab_test_enabled": True,
        "ab_variants": [v.dict() for v in variants]
    })
    
    return {"success": True, "variants": len(variants)}


@router.get("/{campaign_id}/ab-results")
async def get_ab_results(campaign_id: str):
    """Get A/B test results"""
    campaign = campaign_store.get(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    analytics = campaign_store.get_analytics(campaign_id)
    if not analytics:
        return {"results": {}, "winner": None}
    
    ab_results = analytics.get("ab_results", {})
    
    # Determine winner based on conversion rate
    winner = None
    best_rate = 0
    for variant, stats in ab_results.items():
        sent = max(1, stats.get("sent", 1))
        rate = stats.get("converted", 0) / sent
        if rate > best_rate:
            best_rate = rate
            winner = variant
    
    return {
        "results": ab_results,
        "winner": winner,
        "winning_metric": "conversion_rate"
    }


# =========================================================================
# Analytics
# =========================================================================

@router.get("/{campaign_id}/analytics")
async def get_campaign_analytics(campaign_id: str):
    """Get campaign performance analytics"""
    analytics = campaign_store.get_analytics(campaign_id)
    if not analytics:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    # Simulate some activity for demo
    if analytics["opened"] == 0 and analytics["sent"] > 0:
        analytics["opened"] = int(analytics["sent"] * 0.25)
        analytics["clicked"] = int(analytics["opened"] * 0.15)
        analytics["converted"] = int(analytics["clicked"] * 0.10)
        analytics["bounced"] = int(analytics["sent"] * 0.03)
    
    return analytics


@router.post("/{campaign_id}/track/{event}")
async def track_campaign_event(
    campaign_id: str,
    event: str,
    variant: Optional[str] = Query(default=None)
):
    """Track a campaign event (open, click, convert)"""
    valid_events = ["opened", "clicked", "converted", "bounced", "unsubscribed"]
    if event not in valid_events:
        raise HTTPException(status_code=400, detail=f"Invalid event. Must be one of: {valid_events}")
    
    campaign_store.record_event(campaign_id, event, variant)
    return {"success": True}


# =========================================================================
# ROI Calculator
# =========================================================================

@router.post("/{campaign_id}/roi")
async def calculate_campaign_roi(
    campaign_id: str,
    cost_per_send: float = Query(default=0.01),
    average_enrollment_value: float = Query(default=500)
):
    """Calculate ROI for a campaign"""
    analytics = campaign_store.get_analytics(campaign_id)
    if not analytics:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    sent = analytics["sent"]
    converted = analytics.get("converted", 0)
    
    total_cost = sent * cost_per_send
    total_revenue = converted * average_enrollment_value
    profit = total_revenue - total_cost
    roi = (profit / max(1, total_cost)) * 100
    
    return {
        "campaign_id": campaign_id,
        "sent": sent,
        "conversions": converted,
        "total_cost": round(total_cost, 2),
        "total_revenue": round(total_revenue, 2),
        "profit": round(profit, 2),
        "roi_percentage": round(roi, 1),
        "cost_per_conversion": round(total_cost / max(1, converted), 2)
    }
