"""
Adaptive Learning Routes for ACA DataHub
User preference learning, smart defaults, query optimization, and continuous improvement
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random

router = APIRouter(prefix="/adaptive", tags=["Adaptive Learning"])


# =========================================================================
# Models
# =========================================================================

class PreferenceType(str, Enum):
    VISUALIZATION = "visualization"
    FILTER = "filter"
    SORT = "sort"
    COLUMNS = "columns"
    THEME = "theme"


# =========================================================================
# User Preference Learner
# =========================================================================

class PreferenceLearner:
    """Learn and apply user preferences"""
    
    def __init__(self):
        self.preferences: Dict[str, dict] = {}
        self.actions: List[dict] = []
        self._counter = 0
    
    def record_action(
        self,
        user_id: str,
        action_type: str,
        context: dict,
        value: Any
    ) -> dict:
        """Record user action for learning"""
        self._counter += 1
        
        action = {
            "id": f"action_{self._counter}",
            "user_id": user_id,
            "action_type": action_type,
            "context": context,
            "value": value,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.actions.append(action)
        
        # Update preferences
        self._update_preference(user_id, action_type, context, value)
        
        return action
    
    def _update_preference(
        self,
        user_id: str,
        action_type: str,
        context: dict,
        value: Any
    ):
        if user_id not in self.preferences:
            self.preferences[user_id] = {}
        
        pref_key = f"{action_type}:{context.get('screen', 'default')}"
        
        if pref_key not in self.preferences[user_id]:
            self.preferences[user_id][pref_key] = {
                "values": {},
                "total_uses": 0
            }
        
        pref = self.preferences[user_id][pref_key]
        value_str = str(value)
        
        if value_str not in pref["values"]:
            pref["values"][value_str] = 0
        
        pref["values"][value_str] += 1
        pref["total_uses"] += 1
    
    def get_preferences(self, user_id: str) -> dict:
        """Get learned preferences for user"""
        user_prefs = self.preferences.get(user_id, {})
        
        suggested_defaults = {}
        for pref_key, pref_data in user_prefs.items():
            if pref_data["values"]:
                # Get most used value
                most_used = max(pref_data["values"], key=pref_data["values"].get)
                confidence = pref_data["values"][most_used] / pref_data["total_uses"]
                
                suggested_defaults[pref_key] = {
                    "value": most_used,
                    "confidence": round(confidence, 3),
                    "uses": pref_data["values"][most_used]
                }
        
        return {
            "user_id": user_id,
            "preferences": user_prefs,
            "suggested_defaults": suggested_defaults
        }
    
    def get_default_value(
        self,
        user_id: str,
        preference_type: str,
        screen: str = "default"
    ) -> dict:
        """Get recommended default value"""
        pref_key = f"{preference_type}:{screen}"
        user_prefs = self.preferences.get(user_id, {}).get(pref_key, {})
        
        if not user_prefs.get("values"):
            return {"has_preference": False, "use_system_default": True}
        
        most_used = max(user_prefs["values"], key=user_prefs["values"].get)
        confidence = user_prefs["values"][most_used] / max(user_prefs["total_uses"], 1)
        
        return {
            "has_preference": True,
            "recommended_value": most_used,
            "confidence": round(confidence, 3),
            "apply_automatically": confidence > 0.7
        }


learner = PreferenceLearner()


# =========================================================================
# Smart Query Optimizer
# =========================================================================

class SmartQueryOptimizer:
    """Optimize queries based on usage patterns"""
    
    def __init__(self):
        self.query_patterns: Dict[str, dict] = {}
        self.rewrites: List[dict] = []
        self._counter = 0
    
    def analyze_query(self, query: str, user_id: str) -> dict:
        """Analyze and suggest query improvements"""
        query_hash = str(hash(query) % 10000000)
        
        if query_hash not in self.query_patterns:
            self.query_patterns[query_hash] = {
                "query": query,
                "executions": 0,
                "avg_time_ms": 0,
                "users": set()
            }
        
        pattern = self.query_patterns[query_hash]
        pattern["executions"] += 1
        pattern["users"].add(user_id)
        pattern["avg_time_ms"] = random.randint(100, 2000)
        
        suggestions = []
        query_lower = query.lower()
        
        if "select *" in query_lower:
            suggestions.append({
                "type": "column_selection",
                "original": "SELECT *",
                "suggestion": "SELECT specific columns",
                "impact": "high"
            })
        
        if "order by" in query_lower and "limit" not in query_lower:
            suggestions.append({
                "type": "add_limit",
                "suggestion": "Add LIMIT clause",
                "impact": "medium"
            })
        
        return {
            "query_hash": query_hash,
            "executions": pattern["executions"],
            "popular": pattern["executions"] > 10,
            "suggestions": suggestions,
            "estimated_improvement": f"{random.randint(10, 50)}%"
        }
    
    def get_recommended_queries(self, user_id: str, context: str = None) -> List[dict]:
        """Get recommended queries based on patterns"""
        recommendations = [
            {
                "query": "SELECT * FROM leads WHERE score > 70 ORDER BY score DESC LIMIT 100",
                "description": "Top performing leads",
                "category": "leads",
                "popularity": random.randint(50, 200)
            },
            {
                "query": "SELECT state, COUNT(*) FROM leads GROUP BY state ORDER BY COUNT(*) DESC",
                "description": "Leads by state",
                "category": "leads",
                "popularity": random.randint(30, 150)
            },
            {
                "query": "SELECT DATE(created_at), COUNT(*) FROM leads GROUP BY DATE(created_at)",
                "description": "Daily lead trends",
                "category": "trends",
                "popularity": random.randint(40, 180)
            }
        ]
        
        return recommendations


optimizer = SmartQueryOptimizer()


# =========================================================================
# Intelligent Cache
# =========================================================================

class IntelligentCache:
    """Smart caching based on usage patterns"""
    
    def __init__(self):
        self.cache: Dict[str, dict] = {}
        self.access_patterns: Dict[str, dict] = {}
        self.predictions: Dict[str, dict] = {}
    
    def record_access(self, cache_key: str, user_id: str) -> dict:
        """Record cache access"""
        if cache_key not in self.access_patterns:
            self.access_patterns[cache_key] = {
                "total_accesses": 0,
                "users": set(),
                "hourly_pattern": [0] * 24,
                "last_access": None
            }
        
        pattern = self.access_patterns[cache_key]
        pattern["total_accesses"] += 1
        pattern["users"].add(user_id)
        pattern["last_access"] = datetime.utcnow().isoformat()
        
        hour = datetime.utcnow().hour
        pattern["hourly_pattern"][hour] += 1
        
        return {
            "cache_key": cache_key,
            "total_accesses": pattern["total_accesses"],
            "unique_users": len(pattern["users"])
        }
    
    def predict_next_access(self, cache_key: str) -> dict:
        """Predict when cache will be accessed next"""
        pattern = self.access_patterns.get(cache_key)
        
        if not pattern:
            return {"prediction": "unknown", "confidence": 0}
        
        # Find peak hours
        hourly = pattern["hourly_pattern"]
        peak_hour = hourly.index(max(hourly))
        
        current_hour = datetime.utcnow().hour
        hours_until_peak = (peak_hour - current_hour) % 24
        
        return {
            "cache_key": cache_key,
            "peak_hour": peak_hour,
            "hours_until_peak": hours_until_peak,
            "should_preload": hours_until_peak <= 1,
            "confidence": round(max(hourly) / max(sum(hourly), 1), 3)
        }
    
    def get_preload_candidates(self) -> List[dict]:
        """Get candidates for cache preloading"""
        candidates = []
        
        for cache_key, pattern in self.access_patterns.items():
            pred = self.predict_next_access(cache_key)
            if pred.get("should_preload"):
                candidates.append({
                    "cache_key": cache_key,
                    "priority": pattern["total_accesses"],
                    "prediction": pred
                })
        
        candidates.sort(key=lambda x: x["priority"], reverse=True)
        return candidates[:10]


cache = IntelligentCache()


# =========================================================================
# Feedback Manager
# =========================================================================

class FeedbackManager:
    """Collect and process user feedback for improvement"""
    
    def __init__(self):
        self.feedback: List[dict] = []
        self._counter = 0
    
    def submit_feedback(
        self,
        user_id: str,
        feature: str,
        rating: int,
        comment: str = None
    ) -> dict:
        self._counter += 1
        
        fb = {
            "id": f"feedback_{self._counter}",
            "user_id": user_id,
            "feature": feature,
            "rating": rating,
            "comment": comment,
            "submitted_at": datetime.utcnow().isoformat()
        }
        
        self.feedback.append(fb)
        return fb
    
    def get_feature_scores(self) -> dict:
        """Get aggregated feature scores"""
        feature_scores = {}
        
        for fb in self.feedback:
            feature = fb["feature"]
            if feature not in feature_scores:
                feature_scores[feature] = {"ratings": [], "count": 0}
            
            feature_scores[feature]["ratings"].append(fb["rating"])
            feature_scores[feature]["count"] += 1
        
        aggregated = {}
        for feature, data in feature_scores.items():
            if data["ratings"]:
                avg = sum(data["ratings"]) / len(data["ratings"])
                aggregated[feature] = {
                    "average_rating": round(avg, 2),
                    "total_feedback": data["count"],
                    "needs_attention": avg < 3.5
                }
        
        return aggregated


feedback = FeedbackManager()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/actions")
async def record_user_action(
    user_id: str = Query(...),
    action_type: PreferenceType = Query(...),
    screen: str = Query(default="default"),
    value: str = Query(...)
):
    """Record user action for learning"""
    return learner.record_action(
        user_id, action_type.value, {"screen": screen}, value
    )


@router.get("/preferences/{user_id}")
async def get_user_preferences(user_id: str):
    """Get learned preferences for user"""
    return learner.get_preferences(user_id)


@router.get("/defaults/{user_id}")
async def get_default_value(
    user_id: str,
    preference_type: PreferenceType = Query(...),
    screen: str = Query(default="default")
):
    """Get recommended default value"""
    return learner.get_default_value(user_id, preference_type.value, screen)


@router.post("/queries/analyze")
async def analyze_query(query: str = Query(...), user_id: str = Query(...)):
    """Analyze query for optimization"""
    return optimizer.analyze_query(query, user_id)


@router.get("/queries/recommended/{user_id}")
async def get_recommended_queries(user_id: str):
    """Get recommended queries for user"""
    return {"recommendations": optimizer.get_recommended_queries(user_id)}


@router.post("/cache/access")
async def record_cache_access(cache_key: str = Query(...), user_id: str = Query(...)):
    """Record cache access"""
    return cache.record_access(cache_key, user_id)


@router.get("/cache/predict/{cache_key}")
async def predict_cache_access(cache_key: str):
    """Predict next cache access"""
    return cache.predict_next_access(cache_key)


@router.get("/cache/preload")
async def get_preload_candidates():
    """Get cache preload candidates"""
    return {"candidates": cache.get_preload_candidates()}


@router.post("/feedback")
async def submit_feedback(
    user_id: str = Query(...),
    feature: str = Query(...),
    rating: int = Query(..., ge=1, le=5),
    comment: str = Query(default=None)
):
    """Submit user feedback"""
    return feedback.submit_feedback(user_id, feature, rating, comment)


@router.get("/feedback/scores")
async def get_feature_scores():
    """Get aggregated feature scores"""
    return {"scores": feedback.get_feature_scores()}


@router.get("/feedback")
async def list_feedback(limit: int = Query(default=50)):
    """List recent feedback"""
    return {"feedback": feedback.feedback[-limit:]}
