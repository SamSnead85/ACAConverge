"""
Billing & Subscription Routes for ACA DataHub
Multi-tenant SaaS billing, Stripe integration, usage metering
"""

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum

router = APIRouter(prefix="/billing", tags=["Billing"])


# =========================================================================
# Models
# =========================================================================

class PlanTier(str, Enum):
    FREE = "free"
    STARTER = "starter"
    PRO = "pro"
    ENTERPRISE = "enterprise"


class SubscriptionStatus(str, Enum):
    ACTIVE = "active"
    PAST_DUE = "past_due"
    CANCELLED = "cancelled"
    TRIALING = "trialing"


# =========================================================================
# Plans Configuration
# =========================================================================

PLANS = {
    PlanTier.FREE.value: {
        "id": "plan_free",
        "name": "Free",
        "price_monthly": 0,
        "price_yearly": 0,
        "features": {
            "leads": 1000,
            "populations": 5,
            "campaigns_per_month": 2,
            "users": 1,
            "api_calls_per_day": 100,
            "storage_gb": 1,
            "support": "community"
        },
        "limits": {
            "max_leads": 1000,
            "max_populations": 5,
            "max_users": 1
        }
    },
    PlanTier.STARTER.value: {
        "id": "plan_starter",
        "name": "Starter",
        "price_monthly": 49,
        "price_yearly": 490,
        "features": {
            "leads": 10000,
            "populations": 25,
            "campaigns_per_month": 20,
            "users": 5,
            "api_calls_per_day": 5000,
            "storage_gb": 10,
            "support": "email"
        },
        "limits": {
            "max_leads": 10000,
            "max_populations": 25,
            "max_users": 5
        }
    },
    PlanTier.PRO.value: {
        "id": "plan_pro",
        "name": "Professional",
        "price_monthly": 149,
        "price_yearly": 1490,
        "features": {
            "leads": 100000,
            "populations": 100,
            "campaigns_per_month": -1,  # unlimited
            "users": 25,
            "api_calls_per_day": 50000,
            "storage_gb": 100,
            "support": "priority",
            "sso": True,
            "advanced_analytics": True
        },
        "limits": {
            "max_leads": 100000,
            "max_populations": 100,
            "max_users": 25
        }
    },
    PlanTier.ENTERPRISE.value: {
        "id": "plan_enterprise",
        "name": "Enterprise",
        "price_monthly": -1,  # Custom pricing
        "price_yearly": -1,
        "features": {
            "leads": -1,  # unlimited
            "populations": -1,
            "campaigns_per_month": -1,
            "users": -1,
            "api_calls_per_day": -1,
            "storage_gb": -1,
            "support": "dedicated",
            "sso": True,
            "advanced_analytics": True,
            "custom_integrations": True,
            "sla": True,
            "dedicated_infrastructure": True
        },
        "limits": {}  # No limits
    }
}


# =========================================================================
# Billing Store
# =========================================================================

class BillingStore:
    """Manages subscriptions, invoices, and usage"""
    
    def __init__(self):
        self.subscriptions: Dict[str, dict] = {}
        self.invoices: Dict[str, dict] = {}
        self.usage: Dict[str, dict] = {}
        self._sub_counter = 0
        self._inv_counter = 0
    
    def create_subscription(self, org_id: str, plan: str, billing_cycle: str = "monthly") -> dict:
        self._sub_counter += 1
        sub_id = f"sub_{self._sub_counter}"
        
        plan_data = PLANS.get(plan, PLANS[PlanTier.FREE.value])
        price = plan_data["price_monthly"] if billing_cycle == "monthly" else plan_data["price_yearly"]
        
        subscription = {
            "id": sub_id,
            "org_id": org_id,
            "plan": plan,
            "plan_name": plan_data["name"],
            "status": SubscriptionStatus.ACTIVE.value,
            "billing_cycle": billing_cycle,
            "price": price,
            "features": plan_data["features"],
            "limits": plan_data["limits"],
            "created_at": datetime.utcnow().isoformat(),
            "current_period_start": datetime.utcnow().isoformat(),
            "current_period_end": (datetime.utcnow() + timedelta(days=30 if billing_cycle == "monthly" else 365)).isoformat(),
            "cancel_at_period_end": False
        }
        
        self.subscriptions[org_id] = subscription
        
        # Initialize usage tracking
        self.usage[org_id] = {
            "leads": 0,
            "populations": 0,
            "campaigns_this_month": 0,
            "users": 1,
            "api_calls_today": 0,
            "storage_used_gb": 0,
            "last_reset": datetime.utcnow().isoformat()
        }
        
        return subscription
    
    def get_subscription(self, org_id: str) -> Optional[dict]:
        return self.subscriptions.get(org_id)
    
    def update_subscription(self, org_id: str, new_plan: str) -> Optional[dict]:
        if org_id not in self.subscriptions:
            return None
        
        plan_data = PLANS.get(new_plan)
        if not plan_data:
            return None
        
        self.subscriptions[org_id]["plan"] = new_plan
        self.subscriptions[org_id]["plan_name"] = plan_data["name"]
        self.subscriptions[org_id]["features"] = plan_data["features"]
        self.subscriptions[org_id]["limits"] = plan_data["limits"]
        
        return self.subscriptions[org_id]
    
    def cancel_subscription(self, org_id: str, immediate: bool = False) -> Optional[dict]:
        if org_id not in self.subscriptions:
            return None
        
        if immediate:
            self.subscriptions[org_id]["status"] = SubscriptionStatus.CANCELLED.value
        else:
            self.subscriptions[org_id]["cancel_at_period_end"] = True
        
        return self.subscriptions[org_id]
    
    def check_limit(self, org_id: str, resource: str, amount: int = 1) -> dict:
        sub = self.subscriptions.get(org_id)
        usage = self.usage.get(org_id, {})
        
        if not sub:
            return {"allowed": False, "reason": "No subscription"}
        
        limit = sub["limits"].get(f"max_{resource}")
        current = usage.get(resource, 0)
        
        if limit == -1:  # Unlimited
            return {"allowed": True, "remaining": -1}
        
        if limit is None:
            return {"allowed": True, "remaining": -1}
        
        remaining = limit - current
        allowed = (current + amount) <= limit
        
        return {
            "allowed": allowed,
            "current": current,
            "limit": limit,
            "remaining": remaining,
            "resource": resource
        }
    
    def record_usage(self, org_id: str, resource: str, amount: int = 1):
        if org_id not in self.usage:
            self.usage[org_id] = {}
        
        current = self.usage[org_id].get(resource, 0)
        self.usage[org_id][resource] = current + amount
    
    def get_usage(self, org_id: str) -> dict:
        usage = self.usage.get(org_id, {})
        sub = self.subscriptions.get(org_id)
        
        if not sub:
            return {}
        
        # Calculate usage percentages
        result = {}
        for resource, current in usage.items():
            limit = sub["limits"].get(f"max_{resource}")
            if limit and limit > 0:
                result[resource] = {
                    "current": current,
                    "limit": limit,
                    "percentage": round(current / limit * 100, 1)
                }
            else:
                result[resource] = {
                    "current": current,
                    "limit": "unlimited",
                    "percentage": 0
                }
        
        return result
    
    def create_invoice(self, org_id: str, amount: float, description: str) -> dict:
        self._inv_counter += 1
        invoice_id = f"inv_{self._inv_counter}"
        
        invoice = {
            "id": invoice_id,
            "org_id": org_id,
            "amount": amount,
            "currency": "usd",
            "description": description,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "paid_at": None
        }
        
        self.invoices[invoice_id] = invoice
        return invoice
    
    def list_invoices(self, org_id: str) -> List[dict]:
        return [inv for inv in self.invoices.values() if inv["org_id"] == org_id]


billing_store = BillingStore()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/plans")
async def list_plans():
    """List available subscription plans"""
    return {"plans": list(PLANS.values())}


@router.get("/plans/{plan_id}")
async def get_plan(plan_id: str):
    """Get plan details"""
    plan = PLANS.get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return plan


@router.post("/subscribe")
async def create_subscription(
    org_id: str = Query(...),
    plan: PlanTier = Query(default=PlanTier.FREE),
    billing_cycle: str = Query(default="monthly")
):
    """Create a subscription"""
    subscription = billing_store.create_subscription(org_id, plan.value, billing_cycle)
    return {"success": True, "subscription": subscription}


@router.get("/subscription/{org_id}")
async def get_subscription(org_id: str):
    """Get organization subscription"""
    subscription = billing_store.get_subscription(org_id)
    if not subscription:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return subscription


@router.put("/subscription/{org_id}")
async def update_subscription(org_id: str, new_plan: PlanTier):
    """Update subscription plan"""
    result = billing_store.update_subscription(org_id, new_plan.value)
    if not result:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return {"success": True, "subscription": result}


@router.delete("/subscription/{org_id}")
async def cancel_subscription(org_id: str, immediate: bool = Query(default=False)):
    """Cancel subscription"""
    result = billing_store.cancel_subscription(org_id, immediate)
    if not result:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return {"success": True, "subscription": result}


@router.get("/usage/{org_id}")
async def get_usage(org_id: str):
    """Get organization usage"""
    return {"usage": billing_store.get_usage(org_id)}


@router.post("/usage/{org_id}/check")
async def check_usage_limit(
    org_id: str,
    resource: str = Query(...),
    amount: int = Query(default=1)
):
    """Check if usage is within limits"""
    return billing_store.check_limit(org_id, resource, amount)


@router.post("/usage/{org_id}/record")
async def record_usage(
    org_id: str,
    resource: str = Query(...),
    amount: int = Query(default=1)
):
    """Record resource usage"""
    billing_store.record_usage(org_id, resource, amount)
    return {"success": True}


@router.get("/invoices/{org_id}")
async def list_invoices(org_id: str):
    """List organization invoices"""
    return {"invoices": billing_store.list_invoices(org_id)}


@router.post("/invoices")
async def create_invoice(
    org_id: str = Query(...),
    amount: float = Query(...),
    description: str = Query(...)
):
    """Create an invoice"""
    invoice = billing_store.create_invoice(org_id, amount, description)
    return {"success": True, "invoice": invoice}


@router.get("/portal/{org_id}")
async def get_billing_portal_url(org_id: str):
    """Get Stripe billing portal URL"""
    # In production, would create Stripe portal session
    return {
        "portal_url": f"https://billing.stripe.com/p/session/{org_id}",
        "expires_at": (datetime.utcnow() + timedelta(hours=1)).isoformat()
    }


@router.post("/webhook/stripe")
async def handle_stripe_webhook(request: Request):
    """Handle Stripe webhook events"""
    # In production, would verify signature and process events
    payload = await request.json()
    event_type = payload.get("type", "unknown")
    
    return {
        "received": True,
        "event_type": event_type
    }
