"""
Behavioral Analytics Routes for ACA DataHub
User session tracking, funnels, cohorts, retention, and product analytics
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random

router = APIRouter(prefix="/behavioral", tags=["Behavioral Analytics"])


# =========================================================================
# Models
# =========================================================================

class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    CLICK = "click"
    FORM_SUBMIT = "form_submit"
    FEATURE_USE = "feature_use"
    ERROR = "error"


# =========================================================================
# Session Tracker
# =========================================================================

class SessionTracker:
    """Track user sessions and events"""
    
    def __init__(self):
        self.sessions: Dict[str, dict] = {}
        self.events: List[dict] = []
        self._counter = 0
    
    def start_session(self, user_id: str, metadata: dict = None) -> dict:
        self._counter += 1
        session_id = f"session_{self._counter}"
        
        session = {
            "id": session_id,
            "user_id": user_id,
            "started_at": datetime.utcnow().isoformat(),
            "ended_at": None,
            "duration_seconds": None,
            "event_count": 0,
            "pages_viewed": [],
            "metadata": metadata or {}
        }
        
        self.sessions[session_id] = session
        return session
    
    def track_event(
        self,
        session_id: str,
        event_type: str,
        event_name: str,
        properties: dict = None
    ) -> dict:
        if session_id not in self.sessions:
            raise ValueError("Session not found")
        
        self._counter += 1
        
        event = {
            "id": f"event_{self._counter}",
            "session_id": session_id,
            "user_id": self.sessions[session_id]["user_id"],
            "event_type": event_type,
            "event_name": event_name,
            "properties": properties or {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.events.append(event)
        self.sessions[session_id]["event_count"] += 1
        
        if event_type == "page_view":
            self.sessions[session_id]["pages_viewed"].append(event_name)
        
        return event
    
    def end_session(self, session_id: str) -> dict:
        if session_id not in self.sessions:
            raise ValueError("Session not found")
        
        session = self.sessions[session_id]
        session["ended_at"] = datetime.utcnow().isoformat()
        
        # Calculate duration
        start = datetime.fromisoformat(session["started_at"])
        end = datetime.fromisoformat(session["ended_at"])
        session["duration_seconds"] = int((end - start).total_seconds())
        
        return session
    
    def get_session_summary(self, session_id: str) -> dict:
        session = self.sessions.get(session_id)
        if not session:
            raise ValueError("Session not found")
        
        session_events = [e for e in self.events if e["session_id"] == session_id]
        
        return {
            **session,
            "events": session_events,
            "unique_pages": len(set(session["pages_viewed"]))
        }


tracker = SessionTracker()


# =========================================================================
# Funnel Analyzer
# =========================================================================

class FunnelAnalyzer:
    """Analyze conversion funnels"""
    
    def __init__(self):
        self.funnels: Dict[str, dict] = {}
        self._counter = 0
    
    def create_funnel(
        self,
        name: str,
        steps: List[str],
        description: str = None
    ) -> dict:
        self._counter += 1
        funnel_id = f"funnel_{self._counter}"
        
        funnel = {
            "id": funnel_id,
            "name": name,
            "steps": steps,
            "description": description or "",
            "created_at": datetime.utcnow().isoformat()
        }
        
        self.funnels[funnel_id] = funnel
        return funnel
    
    def analyze_funnel(self, funnel_id: str, days: int = 30) -> dict:
        if funnel_id not in self.funnels:
            raise ValueError("Funnel not found")
        
        funnel = self.funnels[funnel_id]
        steps = funnel["steps"]
        
        # Simulate funnel analysis
        step_results = []
        users_remaining = random.randint(1000, 5000)
        
        for i, step in enumerate(steps):
            drop_rate = random.uniform(0.1, 0.4) if i > 0 else 0
            users_remaining = int(users_remaining * (1 - drop_rate))
            
            step_results.append({
                "step": step,
                "order": i + 1,
                "users": users_remaining,
                "conversion_rate": round((1 - drop_rate) * 100 if i > 0 else 100, 2),
                "drop_off": round(drop_rate * 100, 2) if i > 0 else 0
            })
        
        overall_conversion = (step_results[-1]["users"] / step_results[0]["users"]) * 100
        
        return {
            "funnel_id": funnel_id,
            "funnel_name": funnel["name"],
            "period_days": days,
            "steps": step_results,
            "overall_conversion_rate": round(overall_conversion, 2),
            "bottleneck": max(step_results[1:], key=lambda x: x["drop_off"])["step"] if len(step_results) > 1 else None,
            "analyzed_at": datetime.utcnow().isoformat()
        }


funnels = FunnelAnalyzer()


# =========================================================================
# Cohort Analyzer
# =========================================================================

class CohortAnalyzer:
    """Analyze user cohorts and retention"""
    
    def analyze_cohorts(
        self,
        cohort_type: str = "week",
        metric: str = "retention"
    ) -> dict:
        """Analyze cohorts by signup week/month"""
        # Simulate cohort data
        cohorts = []
        today = datetime.utcnow()
        
        for i in range(8):  # 8 cohorts
            cohort_date = (today - timedelta(weeks=i)).strftime("%Y-%W")
            initial_users = random.randint(100, 500)
            
            periods = []
            retention = 100
            
            for p in range(min(i + 1, 8)):
                if p > 0:
                    retention *= random.uniform(0.6, 0.9)
                
                periods.append({
                    "period": p,
                    "users": int(initial_users * retention / 100),
                    "retention_rate": round(retention, 1)
                })
            
            cohorts.append({
                "cohort": cohort_date,
                "initial_users": initial_users,
                "current_retention": round(retention, 1),
                "periods": periods
            })
        
        return {
            "cohort_type": cohort_type,
            "metric": metric,
            "cohorts": cohorts,
            "analyzed_at": datetime.utcnow().isoformat()
        }
    
    def segment_users(
        self,
        criteria: dict
    ) -> dict:
        """Segment users by criteria"""
        segments = [
            {
                "name": "Power Users",
                "criteria": "sessions > 10/week",
                "user_count": random.randint(100, 500),
                "percentage": round(random.uniform(5, 15), 1)
            },
            {
                "name": "Regular Users",
                "criteria": "1-10 sessions/week",
                "user_count": random.randint(500, 2000),
                "percentage": round(random.uniform(30, 50), 1)
            },
            {
                "name": "Casual Users",
                "criteria": "< 1 session/week",
                "user_count": random.randint(200, 1000),
                "percentage": round(random.uniform(20, 35), 1)
            },
            {
                "name": "Churned",
                "criteria": "no activity 30+ days",
                "user_count": random.randint(100, 500),
                "percentage": round(random.uniform(10, 25), 1)
            }
        ]
        
        return {
            "segments": segments,
            "total_users": sum(s["user_count"] for s in segments),
            "segmented_at": datetime.utcnow().isoformat()
        }


cohorts = CohortAnalyzer()


# =========================================================================
# Product Analytics
# =========================================================================

class ProductAnalytics:
    """Product usage and feature analytics"""
    
    def get_feature_adoption(self) -> dict:
        """Get feature adoption metrics"""
        features = [
            {"name": "Dashboard", "adoption_rate": random.randint(80, 100), "category": "core"},
            {"name": "Reports", "adoption_rate": random.randint(60, 90), "category": "core"},
            {"name": "AI Insights", "adoption_rate": random.randint(40, 70), "category": "premium"},
            {"name": "Campaigns", "adoption_rate": random.randint(30, 60), "category": "marketing"},
            {"name": "Integrations", "adoption_rate": random.randint(20, 50), "category": "advanced"},
            {"name": "API Access", "adoption_rate": random.randint(10, 35), "category": "developer"}
        ]
        
        for f in features:
            f["trend"] = random.choice(["growing", "stable", "declining"])
            f["avg_uses_per_user"] = round(random.uniform(1, 20), 1)
        
        return {
            "features": features,
            "most_adopted": max(features, key=lambda x: x["adoption_rate"]),
            "least_adopted": min(features, key=lambda x: x["adoption_rate"]),
            "analyzed_at": datetime.utcnow().isoformat()
        }
    
    def get_engagement_score(self, user_id: str) -> dict:
        """Calculate user engagement score"""
        dimensions = {
            "frequency": random.randint(40, 100),
            "depth": random.randint(30, 100),
            "breadth": random.randint(20, 90),
            "recency": random.randint(50, 100)
        }
        
        overall = sum(dimensions.values()) / len(dimensions)
        
        return {
            "user_id": user_id,
            "overall_score": round(overall, 1),
            "tier": "champion" if overall >= 80 else ("active" if overall >= 50 else "at_risk"),
            "dimensions": dimensions,
            "recommendations": self._get_engagement_recommendations(dimensions)
        }
    
    def _get_engagement_recommendations(self, dimensions: dict) -> List[str]:
        recommendations = []
        
        if dimensions["breadth"] < 50:
            recommendations.append("Encourage exploration of more features")
        if dimensions["depth"] < 50:
            recommendations.append("Provide tutorials for advanced usage")
        if dimensions["recency"] < 50:
            recommendations.append("Send re-engagement campaign")
        
        return recommendations
    
    def predict_churn(self, user_id: str) -> dict:
        """Predict user churn risk"""
        risk_score = random.randint(0, 100)
        
        factors = []
        if risk_score > 50:
            factors.append({"factor": "declining_usage", "impact": random.randint(20, 40)})
        if risk_score > 30:
            factors.append({"factor": "fewer_features_used", "impact": random.randint(10, 30)})
        if random.random() > 0.5:
            factors.append({"factor": "support_tickets", "impact": random.randint(5, 20)})
        
        return {
            "user_id": user_id,
            "churn_risk_score": risk_score,
            "risk_level": "high" if risk_score > 70 else ("medium" if risk_score > 40 else "low"),
            "contributing_factors": factors,
            "recommended_actions": [
                "Personal outreach from success team",
                "Guide to underutilized features",
                "Discount offer for renewal"
            ] if risk_score > 50 else []
        }


analytics = ProductAnalytics()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/sessions/start")
async def start_session(user_id: str = Query(...), metadata: dict = None):
    """Start user session"""
    return tracker.start_session(user_id, metadata)


@router.post("/sessions/{session_id}/event")
async def track_event(
    session_id: str,
    event_type: EventType = Query(...),
    event_name: str = Query(...),
    properties: dict = None
):
    """Track session event"""
    try:
        return tracker.track_event(session_id, event_type.value, event_name, properties)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/sessions/{session_id}/end")
async def end_session(session_id: str):
    """End user session"""
    try:
        return tracker.end_session(session_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/sessions/{session_id}")
async def get_session(session_id: str):
    """Get session details"""
    try:
        return tracker.get_session_summary(session_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/funnels")
async def create_funnel(
    name: str = Query(...),
    steps: List[str] = Query(...)
):
    """Create conversion funnel"""
    return funnels.create_funnel(name, steps)


@router.get("/funnels")
async def list_funnels():
    """List funnels"""
    return {"funnels": list(funnels.funnels.values())}


@router.get("/funnels/{funnel_id}/analyze")
async def analyze_funnel(funnel_id: str, days: int = Query(default=30)):
    """Analyze funnel conversion"""
    try:
        return funnels.analyze_funnel(funnel_id, days)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/cohorts")
async def analyze_cohorts(
    cohort_type: str = Query(default="week"),
    metric: str = Query(default="retention")
):
    """Analyze cohorts"""
    return cohorts.analyze_cohorts(cohort_type, metric)


@router.get("/segments")
async def segment_users(criteria: dict = None):
    """Segment users"""
    return cohorts.segment_users(criteria or {})


@router.get("/features/adoption")
async def get_feature_adoption():
    """Get feature adoption metrics"""
    return analytics.get_feature_adoption()


@router.get("/users/{user_id}/engagement")
async def get_engagement_score(user_id: str):
    """Get user engagement score"""
    return analytics.get_engagement_score(user_id)


@router.get("/users/{user_id}/churn-risk")
async def predict_churn(user_id: str):
    """Predict user churn risk"""
    return analytics.predict_churn(user_id)
