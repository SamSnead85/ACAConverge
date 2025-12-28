"""
Predictive UX & Personalization Routes for ACA DataHub
User behavior analytics, personalization, recommendations, and A/B testing
"""

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import hashlib

router = APIRouter(prefix="/ux", tags=["Predictive UX"])


# =========================================================================
# Models
# =========================================================================

class EventCategory(str, Enum):
    PAGE_VIEW = "page_view"
    CLICK = "click"
    SEARCH = "search"
    FEATURE_USE = "feature_use"
    ERROR = "error"


class ExperimentStatus(str, Enum):
    DRAFT = "draft"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"


# =========================================================================
# User Behavior Analytics
# =========================================================================

class BehaviorAnalytics:
    """Tracks and analyzes user behavior"""
    
    def __init__(self):
        self.events: List[dict] = []
        self.sessions: Dict[str, dict] = {}
        self.user_profiles: Dict[str, dict] = {}
        self._counter = 0
    
    def track_event(
        self,
        user_id: str,
        event_type: str,
        event_data: dict = None,
        session_id: str = None
    ) -> dict:
        self._counter += 1
        event_id = f"evt_{self._counter}"
        
        event = {
            "id": event_id,
            "user_id": user_id,
            "session_id": session_id,
            "type": event_type,
            "data": event_data or {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.events.append(event)
        
        # Update user profile
        self._update_profile(user_id, event)
        
        # Keep last 50000 events
        if len(self.events) > 50000:
            self.events = self.events[-50000:]
        
        return event
    
    def _update_profile(self, user_id: str, event: dict):
        if user_id not in self.user_profiles:
            self.user_profiles[user_id] = {
                "user_id": user_id,
                "event_count": 0,
                "features_used": {},
                "pages_viewed": {},
                "search_queries": [],
                "last_active": None,
                "preferences": {}
            }
        
        profile = self.user_profiles[user_id]
        profile["event_count"] += 1
        profile["last_active"] = event["timestamp"]
        
        # Track feature usage
        if event["type"] == "feature_use":
            feature = event["data"].get("feature", "unknown")
            profile["features_used"][feature] = profile["features_used"].get(feature, 0) + 1
        
        # Track page views
        if event["type"] == "page_view":
            page = event["data"].get("page", "unknown")
            profile["pages_viewed"][page] = profile["pages_viewed"].get(page, 0) + 1
        
        # Track searches
        if event["type"] == "search":
            query = event["data"].get("query")
            if query:
                profile["search_queries"].append(query)
                profile["search_queries"] = profile["search_queries"][-50:]
    
    def get_user_profile(self, user_id: str) -> Optional[dict]:
        return self.user_profiles.get(user_id)
    
    def get_user_events(self, user_id: str, limit: int = 100) -> List[dict]:
        user_events = [e for e in self.events if e["user_id"] == user_id]
        return sorted(user_events, key=lambda x: x["timestamp"], reverse=True)[:limit]
    
    def get_usage_patterns(self, user_id: str) -> dict:
        profile = self.user_profiles.get(user_id, {})
        
        # Determine peak usage times
        events = [e for e in self.events if e["user_id"] == user_id]
        hour_counts = {}
        for e in events:
            try:
                hour = int(e["timestamp"][11:13])
                hour_counts[hour] = hour_counts.get(hour, 0) + 1
            except:
                pass
        
        peak_hour = max(hour_counts.items(), key=lambda x: x[1])[0] if hour_counts else None
        
        # Most used features
        features = profile.get("features_used", {})
        top_features = sorted(features.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            "user_id": user_id,
            "peak_usage_hour": peak_hour,
            "top_features": dict(top_features),
            "total_events": profile.get("event_count", 0),
            "engagement_level": self._calculate_engagement(profile)
        }
    
    def _calculate_engagement(self, profile: dict) -> str:
        event_count = profile.get("event_count", 0)
        if event_count > 100:
            return "high"
        elif event_count > 30:
            return "medium"
        return "low"


behavior_analytics = BehaviorAnalytics()


# =========================================================================
# Personalization Engine
# =========================================================================

class PersonalizationEngine:
    """Provides personalized recommendations and content"""
    
    def __init__(self):
        self.recommendations: Dict[str, List[dict]] = {}
        self.content_cache: Dict[str, dict] = {}
    
    def get_recommendations(
        self,
        user_id: str,
        context: str = None,
        limit: int = 5
    ) -> List[dict]:
        profile = behavior_analytics.get_user_profile(user_id)
        
        if not profile:
            return self._get_default_recommendations(limit)
        
        recommendations = []
        
        # Feature recommendations based on usage
        used_features = set(profile.get("features_used", {}).keys())
        all_features = {"dashboard", "query", "populations", "campaigns", "analytics", 
                       "leads", "reports", "integrations", "ai_assistant"}
        unused = all_features - used_features
        
        for feature in list(unused)[:3]:
            recommendations.append({
                "type": "feature",
                "name": feature,
                "reason": "You haven't tried this feature yet",
                "score": round(random.uniform(0.7, 0.9), 2)
            })
        
        # Content recommendations
        if profile.get("search_queries"):
            recommendations.append({
                "type": "content",
                "name": "Related insights",
                "reason": f"Based on your searches for '{profile['search_queries'][-1]}'",
                "score": round(random.uniform(0.6, 0.85), 2)
            })
        
        return sorted(recommendations, key=lambda x: x["score"], reverse=True)[:limit]
    
    def _get_default_recommendations(self, limit: int) -> List[dict]:
        defaults = [
            {"type": "feature", "name": "dashboard", "reason": "Start here", "score": 0.9},
            {"type": "feature", "name": "query", "reason": "Most popular", "score": 0.85},
            {"type": "content", "name": "Getting Started Guide", "reason": "New user", "score": 0.8}
        ]
        return defaults[:limit]
    
    def get_personalized_dashboard(self, user_id: str) -> dict:
        profile = behavior_analytics.get_user_profile(user_id) or {}
        
        # Determine widget order based on usage
        features = profile.get("features_used", {})
        default_widgets = ["overview", "leads", "campaigns", "analytics", "recent"]
        
        # Sort by usage
        sorted_features = sorted(features.items(), key=lambda x: x[1], reverse=True)
        user_widgets = [f[0] for f in sorted_features if f[0] in default_widgets]
        
        # Add missing widgets
        for w in default_widgets:
            if w not in user_widgets:
                user_widgets.append(w)
        
        return {
            "user_id": user_id,
            "layout": "personalized" if profile else "default",
            "widgets": user_widgets[:6],
            "theme": profile.get("preferences", {}).get("theme", "dark"),
            "quick_actions": self._get_quick_actions(profile)
        }
    
    def _get_quick_actions(self, profile: dict) -> List[dict]:
        features = profile.get("features_used", {})
        top_features = sorted(features.items(), key=lambda x: x[1], reverse=True)[:3]
        
        return [
            {"action": f[0], "label": f"Open {f[0].title()}", "usage": f[1]}
            for f in top_features
        ]
    
    def predictive_search(self, user_id: str, partial_query: str) -> List[dict]:
        """Predictive autocomplete based on user history"""
        profile = behavior_analytics.get_user_profile(user_id) or {}
        
        suggestions = []
        
        # Previous searches
        for q in reversed(profile.get("search_queries", [])[-10:]):
            if partial_query.lower() in q.lower():
                suggestions.append({
                    "text": q,
                    "type": "history",
                    "score": 0.9
                })
        
        # Common queries
        common = ["lead score > 70", "state = 'Georgia'", "income > 50000", "email LIKE '%@%'"]
        for q in common:
            if partial_query.lower() in q.lower():
                suggestions.append({
                    "text": q,
                    "type": "common",
                    "score": 0.7
                })
        
        return sorted(suggestions, key=lambda x: x["score"], reverse=True)[:5]


personalization_engine = PersonalizationEngine()


# =========================================================================
# A/B Testing
# =========================================================================

class ABTestingEngine:
    """Manages A/B tests for UI experiments"""
    
    def __init__(self):
        self.experiments: Dict[str, dict] = {}
        self.assignments: Dict[str, Dict[str, str]] = {}  # user_id -> experiment_id -> variant
        self.conversions: Dict[str, Dict[str, int]] = {}  # experiment_id -> variant -> count
        self._counter = 0
    
    def create_experiment(
        self,
        name: str,
        variants: List[str],
        traffic_percentage: int = 100
    ) -> dict:
        self._counter += 1
        exp_id = f"exp_{self._counter}"
        
        experiment = {
            "id": exp_id,
            "name": name,
            "variants": variants,
            "traffic_percentage": traffic_percentage,
            "status": ExperimentStatus.DRAFT.value,
            "created_at": datetime.utcnow().isoformat(),
            "started_at": None,
            "ended_at": None,
            "variant_counts": {v: 0 for v in variants}
        }
        
        self.experiments[exp_id] = experiment
        self.conversions[exp_id] = {v: 0 for v in variants}
        return experiment
    
    def start_experiment(self, experiment_id: str) -> dict:
        if experiment_id not in self.experiments:
            raise ValueError("Experiment not found")
        
        self.experiments[experiment_id]["status"] = ExperimentStatus.RUNNING.value
        self.experiments[experiment_id]["started_at"] = datetime.utcnow().isoformat()
        return self.experiments[experiment_id]
    
    def get_variant(self, user_id: str, experiment_id: str) -> Optional[str]:
        if experiment_id not in self.experiments:
            return None
        
        experiment = self.experiments[experiment_id]
        
        if experiment["status"] != ExperimentStatus.RUNNING.value:
            return experiment["variants"][0]  # Control
        
        # Check existing assignment
        if user_id in self.assignments and experiment_id in self.assignments[user_id]:
            return self.assignments[user_id][experiment_id]
        
        # Deterministic assignment based on user_id hash
        hash_val = int(hashlib.md5(f"{user_id}:{experiment_id}".encode()).hexdigest()[:8], 16)
        
        # Check if user is in test traffic
        if (hash_val % 100) >= experiment["traffic_percentage"]:
            return experiment["variants"][0]  # Control for excluded users
        
        variant_index = hash_val % len(experiment["variants"])
        variant = experiment["variants"][variant_index]
        
        # Record assignment
        if user_id not in self.assignments:
            self.assignments[user_id] = {}
        self.assignments[user_id][experiment_id] = variant
        
        experiment["variant_counts"][variant] += 1
        
        return variant
    
    def record_conversion(self, experiment_id: str, variant: str):
        if experiment_id in self.conversions:
            self.conversions[experiment_id][variant] = self.conversions[experiment_id].get(variant, 0) + 1
    
    def get_results(self, experiment_id: str) -> dict:
        if experiment_id not in self.experiments:
            raise ValueError("Experiment not found")
        
        experiment = self.experiments[experiment_id]
        conversions = self.conversions.get(experiment_id, {})
        
        results = []
        for variant in experiment["variants"]:
            count = experiment["variant_counts"].get(variant, 0)
            conv = conversions.get(variant, 0)
            rate = round(conv / count * 100, 2) if count > 0 else 0
            
            results.append({
                "variant": variant,
                "participants": count,
                "conversions": conv,
                "conversion_rate": rate
            })
        
        return {
            "experiment_id": experiment_id,
            "name": experiment["name"],
            "status": experiment["status"],
            "results": results
        }


ab_testing = ABTestingEngine()


# =========================================================================
# Endpoints
# =========================================================================

# Behavior Analytics
@router.post("/events")
async def track_event(
    user_id: str = Query(...),
    event_type: EventCategory = Query(...),
    session_id: Optional[str] = Query(default=None),
    data: dict = None
):
    """Track user behavior event"""
    event = behavior_analytics.track_event(user_id, event_type.value, data, session_id)
    return {"success": True, "event": event}


@router.get("/users/{user_id}/profile")
async def get_user_profile(user_id: str):
    """Get user behavior profile"""
    profile = behavior_analytics.get_user_profile(user_id)
    if not profile:
        raise HTTPException(status_code=404, detail="User profile not found")
    return profile


@router.get("/users/{user_id}/events")
async def get_user_events(user_id: str, limit: int = Query(default=100)):
    """Get user event history"""
    return {"events": behavior_analytics.get_user_events(user_id, limit)}


@router.get("/users/{user_id}/patterns")
async def get_usage_patterns(user_id: str):
    """Get user usage patterns"""
    return behavior_analytics.get_usage_patterns(user_id)


# Personalization
@router.get("/users/{user_id}/recommendations")
async def get_recommendations(
    user_id: str,
    context: Optional[str] = Query(default=None),
    limit: int = Query(default=5)
):
    """Get personalized recommendations"""
    return {"recommendations": personalization_engine.get_recommendations(user_id, context, limit)}


@router.get("/users/{user_id}/dashboard")
async def get_personalized_dashboard(user_id: str):
    """Get personalized dashboard configuration"""
    return personalization_engine.get_personalized_dashboard(user_id)


@router.get("/users/{user_id}/search-suggestions")
async def get_search_suggestions(user_id: str, query: str = Query(...)):
    """Get predictive search suggestions"""
    return {"suggestions": personalization_engine.predictive_search(user_id, query)}


# A/B Testing
@router.post("/experiments")
async def create_experiment(
    name: str = Query(...),
    variants: List[str] = Query(default=["control", "variant_a"]),
    traffic_percentage: int = Query(default=100, ge=1, le=100)
):
    """Create A/B test experiment"""
    experiment = ab_testing.create_experiment(name, variants, traffic_percentage)
    return {"success": True, "experiment": experiment}


@router.get("/experiments")
async def list_experiments():
    """List all experiments"""
    return {"experiments": list(ab_testing.experiments.values())}


@router.post("/experiments/{experiment_id}/start")
async def start_experiment(experiment_id: str):
    """Start an experiment"""
    try:
        experiment = ab_testing.start_experiment(experiment_id)
        return {"success": True, "experiment": experiment}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/experiments/{experiment_id}/variant")
async def get_experiment_variant(experiment_id: str, user_id: str = Query(...)):
    """Get assigned variant for user"""
    variant = ab_testing.get_variant(user_id, experiment_id)
    return {"experiment_id": experiment_id, "variant": variant}


@router.post("/experiments/{experiment_id}/convert")
async def record_conversion(experiment_id: str, variant: str = Query(...)):
    """Record conversion for experiment"""
    ab_testing.record_conversion(experiment_id, variant)
    return {"success": True}


@router.get("/experiments/{experiment_id}/results")
async def get_experiment_results(experiment_id: str):
    """Get experiment results"""
    try:
        return ab_testing.get_results(experiment_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
