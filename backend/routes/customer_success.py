"""
Customer Success Platform Routes for ACA DataHub
Health scoring, churn prediction, NPS, journey mapping, and success playbooks
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random

router = APIRouter(prefix="/customer-success", tags=["Customer Success"])


# =========================================================================
# Models
# =========================================================================

class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    AT_RISK = "at_risk"
    CRITICAL = "critical"


class JourneyStage(str, Enum):
    ONBOARDING = "onboarding"
    ADOPTION = "adoption"
    EXPANSION = "expansion"
    RENEWAL = "renewal"
    ADVOCACY = "advocacy"


# =========================================================================
# Customer Success Store
# =========================================================================

class CustomerSuccessStore:
    """Manages customer health and success metrics"""
    
    def __init__(self):
        self.customers: Dict[str, dict] = {}
        self.health_history: Dict[str, List[dict]] = {}
        self.playbooks: Dict[str, dict] = {}
        self.nps_responses: List[dict] = []
        self._counter = 0
        self._init_playbooks()
    
    def _init_playbooks(self):
        self.playbooks = {
            "pb_onboarding": {
                "id": "pb_onboarding",
                "name": "New Customer Onboarding",
                "trigger": "new_customer",
                "steps": [
                    {"day": 0, "action": "Send welcome email", "owner": "CSM"},
                    {"day": 1, "action": "Schedule kickoff call", "owner": "CSM"},
                    {"day": 7, "action": "First check-in", "owner": "CSM"},
                    {"day": 14, "action": "Training session", "owner": "Support"},
                    {"day": 30, "action": "Success review", "owner": "CSM"}
                ],
                "success_criteria": ["First login", "Data uploaded", "Query executed"]
            },
            "pb_churn_risk": {
                "id": "pb_churn_risk",
                "name": "Churn Risk Intervention",
                "trigger": "health_score_below_40",
                "steps": [
                    {"day": 0, "action": "Alert CSM", "owner": "System"},
                    {"day": 1, "action": "Reach out to customer", "owner": "CSM"},
                    {"day": 3, "action": "Executive sponsor call", "owner": "Manager"},
                    {"day": 7, "action": "Action plan review", "owner": "CSM"}
                ],
                "success_criteria": ["Engagement increased", "Issues resolved"]
            },
            "pb_expansion": {
                "id": "pb_expansion",
                "name": "Expansion Opportunity",
                "trigger": "high_usage_signals",
                "steps": [
                    {"day": 0, "action": "Identify expansion signals", "owner": "System"},
                    {"day": 3, "action": "Schedule value review", "owner": "CSM"},
                    {"day": 7, "action": "Present upgrade options", "owner": "Sales"},
                    {"day": 14, "action": "Close expansion deal", "owner": "Sales"}
                ],
                "success_criteria": ["Upgrade completed", "New features adopted"]
            }
        }
    
    def create_customer(self, data: dict) -> dict:
        self._counter += 1
        customer_id = data.get("id") or f"cust_{self._counter}"
        
        customer = {
            "id": customer_id,
            "health_score": 80,
            "health_status": HealthStatus.HEALTHY.value,
            "journey_stage": JourneyStage.ONBOARDING.value,
            "created_at": datetime.utcnow().isoformat(),
            "last_activity": datetime.utcnow().isoformat(),
            "csm_id": None,
            "renewal_date": (datetime.utcnow() + timedelta(days=365)).isoformat(),
            "arr": data.get("arr", 0),
            "stakeholders": [],
            "risks": [],
            "opportunities": [],
            **data
        }
        
        self.customers[customer_id] = customer
        self.health_history[customer_id] = []
        return customer
    
    def calculate_health_score(self, customer_id: str) -> dict:
        if customer_id not in self.customers:
            raise ValueError("Customer not found")
        
        customer = self.customers[customer_id]
        
        # Simulate health score components
        components = {
            "product_usage": random.randint(60, 100),
            "engagement": random.randint(50, 100),
            "support_tickets": random.randint(70, 100),
            "nps_score": random.randint(50, 100),
            "feature_adoption": random.randint(40, 100)
        }
        
        weights = {
            "product_usage": 0.30,
            "engagement": 0.25,
            "support_tickets": 0.15,
            "nps_score": 0.15,
            "feature_adoption": 0.15
        }
        
        health_score = sum(components[k] * weights[k] for k in components)
        health_score = round(health_score, 1)
        
        # Determine status
        if health_score >= 70:
            status = HealthStatus.HEALTHY.value
        elif health_score >= 40:
            status = HealthStatus.AT_RISK.value
        else:
            status = HealthStatus.CRITICAL.value
        
        # Update customer
        customer["health_score"] = health_score
        customer["health_status"] = status
        
        # Record history
        self.health_history[customer_id].append({
            "timestamp": datetime.utcnow().isoformat(),
            "score": health_score,
            "status": status,
            "components": components
        })
        
        return {
            "customer_id": customer_id,
            "health_score": health_score,
            "health_status": status,
            "components": components,
            "trend": self._calculate_trend(customer_id)
        }
    
    def _calculate_trend(self, customer_id: str) -> str:
        history = self.health_history.get(customer_id, [])
        if len(history) < 2:
            return "stable"
        
        recent = history[-1]["score"]
        previous = history[-2]["score"]
        
        if recent > previous + 5:
            return "improving"
        elif recent < previous - 5:
            return "declining"
        return "stable"
    
    def predict_churn(self, customer_id: str) -> dict:
        if customer_id not in self.customers:
            raise ValueError("Customer not found")
        
        customer = self.customers[customer_id]
        health = customer.get("health_score", 50)
        
        # Simulate churn prediction
        base_risk = (100 - health) / 100
        risk = min(0.95, base_risk + random.uniform(-0.1, 0.1))
        
        risk_factors = []
        if health < 50:
            risk_factors.append("Low health score")
        if random.random() > 0.5:
            risk_factors.append("Declining engagement")
        if random.random() > 0.7:
            risk_factors.append("Support escalations")
        
        return {
            "customer_id": customer_id,
            "churn_probability": round(risk, 3),
            "risk_level": "high" if risk > 0.6 else ("medium" if risk > 0.3 else "low"),
            "risk_factors": risk_factors,
            "recommended_actions": [
                "Schedule executive business review",
                "Offer additional training",
                "Review contract terms"
            ] if risk > 0.5 else ["Continue regular engagement"],
            "days_to_renewal": (datetime.fromisoformat(customer["renewal_date"].replace("Z", "")) - datetime.utcnow()).days
        }
    
    def get_journey_map(self, customer_id: str) -> dict:
        if customer_id not in self.customers:
            raise ValueError("Customer not found")
        
        customer = self.customers[customer_id]
        
        # Simulate journey events
        events = [
            {"date": customer["created_at"], "event": "Contract signed", "stage": "onboarding"},
            {"date": (datetime.fromisoformat(customer["created_at"].replace("Z", "")) + timedelta(days=1)).isoformat(), "event": "Welcome call", "stage": "onboarding"},
            {"date": (datetime.fromisoformat(customer["created_at"].replace("Z", "")) + timedelta(days=7)).isoformat(), "event": "First data upload", "stage": "adoption"},
            {"date": (datetime.fromisoformat(customer["created_at"].replace("Z", "")) + timedelta(days=14)).isoformat(), "event": "First campaign sent", "stage": "adoption"},
        ]
        
        return {
            "customer_id": customer_id,
            "current_stage": customer["journey_stage"],
            "events": events,
            "milestones": {
                "onboarding": True,
                "first_value": True,
                "expansion": False,
                "advocacy": False
            },
            "next_milestone": "Feature expansion"
        }
    
    def record_nps(self, customer_id: str, score: int, feedback: str = None) -> dict:
        response = {
            "customer_id": customer_id,
            "score": score,
            "feedback": feedback,
            "category": "promoter" if score >= 9 else ("passive" if score >= 7 else "detractor"),
            "recorded_at": datetime.utcnow().isoformat()
        }
        
        self.nps_responses.append(response)
        return response
    
    def get_nps_summary(self) -> dict:
        if not self.nps_responses:
            return {"nps_score": 0, "responses": 0}
        
        promoters = sum(1 for r in self.nps_responses if r["score"] >= 9)
        detractors = sum(1 for r in self.nps_responses if r["score"] <= 6)
        total = len(self.nps_responses)
        
        nps = round((promoters - detractors) / total * 100, 1)
        
        return {
            "nps_score": nps,
            "responses": total,
            "promoters": promoters,
            "passives": total - promoters - detractors,
            "detractors": detractors
        }


cs_store = CustomerSuccessStore()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/customers")
async def create_customer(
    name: str = Query(...),
    company: str = Query(...),
    arr: float = Query(default=0),
    csm_id: Optional[str] = Query(default=None)
):
    """Create a customer record"""
    customer = cs_store.create_customer({
        "name": name,
        "company": company,
        "arr": arr,
        "csm_id": csm_id
    })
    return {"success": True, "customer": customer}


@router.get("/customers")
async def list_customers(
    health_status: Optional[str] = Query(default=None),
    journey_stage: Optional[str] = Query(default=None)
):
    """List all customers"""
    customers = list(cs_store.customers.values())
    
    if health_status:
        customers = [c for c in customers if c.get("health_status") == health_status]
    if journey_stage:
        customers = [c for c in customers if c.get("journey_stage") == journey_stage]
    
    return {"customers": customers}


@router.get("/customers/{customer_id}")
async def get_customer(customer_id: str):
    """Get customer details"""
    customer = cs_store.customers.get(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


@router.get("/customers/{customer_id}/health")
async def get_health_score(customer_id: str):
    """Calculate and get customer health score"""
    try:
        return cs_store.calculate_health_score(customer_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/customers/{customer_id}/churn-risk")
async def get_churn_risk(customer_id: str):
    """Predict churn risk for customer"""
    try:
        return cs_store.predict_churn(customer_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/customers/{customer_id}/journey")
async def get_customer_journey(customer_id: str):
    """Get customer journey map"""
    try:
        return cs_store.get_journey_map(customer_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/nps")
async def record_nps_response(
    customer_id: str = Query(...),
    score: int = Query(..., ge=0, le=10),
    feedback: Optional[str] = Query(default=None)
):
    """Record NPS response"""
    response = cs_store.record_nps(customer_id, score, feedback)
    return {"success": True, "response": response}


@router.get("/nps/summary")
async def get_nps_summary():
    """Get NPS summary stats"""
    return cs_store.get_nps_summary()


@router.get("/playbooks")
async def list_playbooks():
    """List success playbooks"""
    return {"playbooks": list(cs_store.playbooks.values())}


@router.get("/playbooks/{playbook_id}")
async def get_playbook(playbook_id: str):
    """Get playbook details"""
    playbook = cs_store.playbooks.get(playbook_id)
    if not playbook:
        raise HTTPException(status_code=404, detail="Playbook not found")
    return playbook


@router.get("/dashboard")
async def get_cs_dashboard():
    """Get customer success dashboard metrics"""
    customers = list(cs_store.customers.values())
    
    healthy = sum(1 for c in customers if c.get("health_status") == "healthy")
    at_risk = sum(1 for c in customers if c.get("health_status") == "at_risk")
    critical = sum(1 for c in customers if c.get("health_status") == "critical")
    
    total_arr = sum(c.get("arr", 0) for c in customers)
    at_risk_arr = sum(c.get("arr", 0) for c in customers if c.get("health_status") != "healthy")
    
    return {
        "total_customers": len(customers),
        "health_distribution": {
            "healthy": healthy,
            "at_risk": at_risk,
            "critical": critical
        },
        "arr": {
            "total": total_arr,
            "at_risk": at_risk_arr
        },
        "nps": cs_store.get_nps_summary(),
        "renewals_next_90_days": sum(1 for c in customers if c.get("renewal_date") and 
            (datetime.fromisoformat(c["renewal_date"].replace("Z", "")) - datetime.utcnow()).days <= 90)
    }
