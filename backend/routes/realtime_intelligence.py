"""
Real-Time Intelligence Routes for ACA DataHub
Complex event processing, streaming analytics, and live dashboards
"""

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import asyncio

router = APIRouter(prefix="/realtime", tags=["Real-Time Intelligence"])


# =========================================================================
# Models
# =========================================================================

class WindowType(str, Enum):
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    HOPPING = "hopping"


class AggregationType(str, Enum):
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


class AlertSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# =========================================================================
# Complex Event Processing Engine
# =========================================================================

class CEPEngine:
    """Complex Event Processing engine for pattern matching"""
    
    def __init__(self):
        self.patterns: Dict[str, dict] = {}
        self.matches: List[dict] = []
        self.event_buffer: List[dict] = []
        self._counter = 0
        self._init_patterns()
    
    def _init_patterns(self):
        self.patterns = {
            "rapid_lead_creation": {
                "id": "rapid_lead_creation",
                "name": "Rapid Lead Creation",
                "description": "More than 100 leads created within 5 minutes",
                "condition": {"event_type": "lead.created", "count": 100, "window_minutes": 5},
                "action": "alert"
            },
            "churn_risk_sequence": {
                "id": "churn_risk_sequence",
                "name": "Churn Risk Sequence",
                "description": "User shows declining engagement pattern",
                "condition": {"sequence": ["login", "no_activity_7d", "support_ticket"]},
                "action": "workflow"
            },
            "high_value_lead": {
                "id": "high_value_lead",
                "name": "High-Value Lead Identified",
                "description": "Lead score exceeds 90",
                "condition": {"event_type": "lead.scored", "threshold": 90},
                "action": "notify"
            }
        }
    
    def add_pattern(self, pattern_id: str, pattern: dict) -> dict:
        self._counter += 1
        pattern["id"] = pattern_id or f"pattern_{self._counter}"
        self.patterns[pattern["id"]] = pattern
        return pattern
    
    def process_event(self, event: dict) -> List[dict]:
        """Process event and check for pattern matches"""
        self.event_buffer.append({
            **event,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Keep buffer size manageable
        if len(self.event_buffer) > 10000:
            self.event_buffer = self.event_buffer[-10000:]
        
        matches = []
        for pattern_id, pattern in self.patterns.items():
            if self._check_pattern(pattern, event):
                match = {
                    "pattern_id": pattern_id,
                    "pattern_name": pattern["name"],
                    "event": event,
                    "matched_at": datetime.utcnow().isoformat(),
                    "action": pattern.get("action", "log")
                }
                matches.append(match)
                self.matches.append(match)
        
        return matches
    
    def _check_pattern(self, pattern: dict, event: dict) -> bool:
        condition = pattern.get("condition", {})
        
        # Check event type match
        if "event_type" in condition:
            if event.get("type") != condition["event_type"]:
                return False
        
        # Check threshold
        if "threshold" in condition:
            value = event.get("data", {}).get("score", 0)
            if value < condition["threshold"]:
                return False
        
        # Check count within window
        if "count" in condition and "window_minutes" in condition:
            cutoff = datetime.utcnow() - timedelta(minutes=condition["window_minutes"])
            recent_count = sum(
                1 for e in self.event_buffer
                if e.get("type") == condition["event_type"]
                and e["timestamp"] > cutoff.isoformat()
            )
            if recent_count < condition["count"]:
                return False
        
        return random.random() > 0.95  # Simulate occasional matches


cep_engine = CEPEngine()


# =========================================================================
# Streaming Aggregations
# =========================================================================

class StreamAggregator:
    """Real-time aggregations over event streams"""
    
    def __init__(self):
        self.aggregations: Dict[str, dict] = {}
        self.results: Dict[str, List[dict]] = {}
        self._counter = 0
    
    def create_aggregation(
        self,
        name: str,
        event_type: str,
        agg_type: str,
        field: str,
        window_type: str,
        window_size_seconds: int,
        group_by: List[str] = None
    ) -> dict:
        self._counter += 1
        agg_id = f"agg_{self._counter}"
        
        aggregation = {
            "id": agg_id,
            "name": name,
            "event_type": event_type,
            "aggregation_type": agg_type,
            "field": field,
            "window_type": window_type,
            "window_size_seconds": window_size_seconds,
            "group_by": group_by or [],
            "created_at": datetime.utcnow().isoformat()
        }
        
        self.aggregations[agg_id] = aggregation
        self.results[agg_id] = []
        return aggregation
    
    def get_current_value(self, agg_id: str) -> dict:
        if agg_id not in self.aggregations:
            return {}
        
        agg = self.aggregations[agg_id]
        
        # Simulate aggregation result
        result = {
            "aggregation_id": agg_id,
            "timestamp": datetime.utcnow().isoformat(),
            "window_start": (datetime.utcnow() - timedelta(seconds=agg["window_size_seconds"])).isoformat(),
            "window_end": datetime.utcnow().isoformat(),
            "value": round(random.uniform(10, 1000), 2)
        }
        
        self.results[agg_id].append(result)
        return result
    
    def get_history(self, agg_id: str, limit: int = 10) -> List[dict]:
        return self.results.get(agg_id, [])[-limit:]


stream_aggregator = StreamAggregator()


# =========================================================================
# Real-Time Decision Engine
# =========================================================================

class DecisionEngine:
    """Real-time automated decision making"""
    
    def __init__(self):
        self.rules: Dict[str, dict] = {}
        self.decisions: List[dict] = []
        self._counter = 0
    
    def add_rule(
        self,
        name: str,
        conditions: List[dict],
        action: str,
        priority: int = 5
    ) -> dict:
        self._counter += 1
        rule_id = f"rule_{self._counter}"
        
        rule = {
            "id": rule_id,
            "name": name,
            "conditions": conditions,
            "action": action,
            "priority": priority,
            "enabled": True,
            "created_at": datetime.utcnow().isoformat()
        }
        
        self.rules[rule_id] = rule
        return rule
    
    def evaluate(self, data: dict) -> dict:
        """Evaluate data against rules and make decision"""
        applicable_rules = []
        
        for rule_id, rule in self.rules.items():
            if not rule["enabled"]:
                continue
            
            if self._check_conditions(rule["conditions"], data):
                applicable_rules.append(rule)
        
        if not applicable_rules:
            return {"decision": "no_action", "rules_matched": 0}
        
        # Sort by priority and take highest
        applicable_rules.sort(key=lambda r: r["priority"], reverse=True)
        winning_rule = applicable_rules[0]
        
        decision = {
            "decision": winning_rule["action"],
            "rule_id": winning_rule["id"],
            "rule_name": winning_rule["name"],
            "rules_matched": len(applicable_rules),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.decisions.append(decision)
        return decision
    
    def _check_conditions(self, conditions: List[dict], data: dict) -> bool:
        for condition in conditions:
            field = condition.get("field")
            operator = condition.get("operator", "eq")
            value = condition.get("value")
            
            actual_value = data.get(field)
            
            if operator == "eq" and actual_value != value:
                return False
            elif operator == "gt" and (actual_value is None or actual_value <= value):
                return False
            elif operator == "lt" and (actual_value is None or actual_value >= value):
                return False
            elif operator == "in" and actual_value not in value:
                return False
        
        return True


decision_engine = DecisionEngine()


# =========================================================================
# Alert Manager
# =========================================================================

class AlertManager:
    """Real-time alerting system"""
    
    def __init__(self):
        self.alert_rules: Dict[str, dict] = {}
        self.active_alerts: List[dict] = []
        self.alert_history: List[dict] = []
        self._counter = 0
    
    def create_rule(
        self,
        name: str,
        metric: str,
        condition: str,
        threshold: float,
        severity: str,
        channels: List[str] = None
    ) -> dict:
        self._counter += 1
        rule_id = f"alert_rule_{self._counter}"
        
        rule = {
            "id": rule_id,
            "name": name,
            "metric": metric,
            "condition": condition,  # "gt", "lt", "eq"
            "threshold": threshold,
            "severity": severity,
            "channels": channels or ["email"],
            "enabled": True
        }
        
        self.alert_rules[rule_id] = rule
        return rule
    
    def trigger_alert(self, rule_id: str, current_value: float, context: dict = None) -> dict:
        if rule_id not in self.alert_rules:
            return {}
        
        rule = self.alert_rules[rule_id]
        self._counter += 1
        alert_id = f"alert_{self._counter}"
        
        alert = {
            "id": alert_id,
            "rule_id": rule_id,
            "rule_name": rule["name"],
            "severity": rule["severity"],
            "current_value": current_value,
            "threshold": rule["threshold"],
            "context": context or {},
            "triggered_at": datetime.utcnow().isoformat(),
            "acknowledged": False,
            "resolved": False
        }
        
        self.active_alerts.append(alert)
        self.alert_history.append(alert)
        return alert
    
    def acknowledge(self, alert_id: str) -> bool:
        for alert in self.active_alerts:
            if alert["id"] == alert_id:
                alert["acknowledged"] = True
                alert["acknowledged_at"] = datetime.utcnow().isoformat()
                return True
        return False
    
    def resolve(self, alert_id: str) -> bool:
        for i, alert in enumerate(self.active_alerts):
            if alert["id"] == alert_id:
                alert["resolved"] = True
                alert["resolved_at"] = datetime.utcnow().isoformat()
                self.active_alerts.pop(i)
                return True
        return False


alert_manager = AlertManager()


# =========================================================================
# Time-Series Analytics
# =========================================================================

class TimeSeriesAnalytics:
    """Time-series analysis and forecasting"""
    
    def __init__(self):
        self.series: Dict[str, List[dict]] = {}
    
    def ingest(self, series_id: str, value: float, timestamp: str = None):
        if series_id not in self.series:
            self.series[series_id] = []
        
        point = {
            "timestamp": timestamp or datetime.utcnow().isoformat(),
            "value": value
        }
        self.series[series_id].append(point)
        
        # Keep last 10000 points
        if len(self.series[series_id]) > 10000:
            self.series[series_id] = self.series[series_id][-10000:]
    
    def get_series(self, series_id: str, limit: int = 100) -> List[dict]:
        return self.series.get(series_id, [])[-limit:]
    
    def forecast(self, series_id: str, periods: int = 10) -> List[dict]:
        """Simple forecasting"""
        history = self.series.get(series_id, [])
        
        if len(history) < 10:
            return []
        
        # Calculate simple moving average and trend
        recent = [p["value"] for p in history[-20:]]
        avg = sum(recent) / len(recent)
        
        if len(recent) > 1:
            trend = (recent[-1] - recent[0]) / len(recent)
        else:
            trend = 0
        
        forecasts = []
        last_ts = datetime.fromisoformat(history[-1]["timestamp"].replace("Z", ""))
        
        for i in range(periods):
            forecast_ts = last_ts + timedelta(hours=i+1)
            forecast_val = avg + trend * (i + 1) + random.uniform(-5, 5)
            forecasts.append({
                "timestamp": forecast_ts.isoformat(),
                "value": round(forecast_val, 2),
                "confidence_lower": round(forecast_val - 10, 2),
                "confidence_upper": round(forecast_val + 10, 2)
            })
        
        return forecasts


time_series = TimeSeriesAnalytics()


# =========================================================================
# Endpoints
# =========================================================================

# CEP Endpoints
@router.get("/patterns")
async def list_patterns():
    """List CEP patterns"""
    return {"patterns": list(cep_engine.patterns.values())}


@router.post("/patterns")
async def create_pattern(
    name: str = Query(...),
    description: str = Query(default=""),
    condition: dict = None,
    action: str = Query(default="alert")
):
    """Create CEP pattern"""
    pattern = cep_engine.add_pattern(None, {
        "name": name,
        "description": description,
        "condition": condition or {},
        "action": action
    })
    return {"success": True, "pattern": pattern}


@router.post("/events/process")
async def process_event(event_type: str = Query(...), data: dict = None):
    """Process event through CEP engine"""
    event = {"type": event_type, "data": data or {}}
    matches = cep_engine.process_event(event)
    return {"event": event, "matches": matches}


@router.get("/matches")
async def get_pattern_matches(limit: int = Query(default=50)):
    """Get recent pattern matches"""
    return {"matches": cep_engine.matches[-limit:]}


# Aggregation Endpoints
@router.post("/aggregations")
async def create_aggregation(
    name: str = Query(...),
    event_type: str = Query(...),
    agg_type: AggregationType = Query(...),
    field: str = Query(...),
    window_type: WindowType = Query(default=WindowType.TUMBLING),
    window_size: int = Query(default=60)
):
    """Create streaming aggregation"""
    agg = stream_aggregator.create_aggregation(
        name, event_type, agg_type.value, field,
        window_type.value, window_size
    )
    return {"success": True, "aggregation": agg}


@router.get("/aggregations")
async def list_aggregations():
    """List aggregations"""
    return {"aggregations": list(stream_aggregator.aggregations.values())}


@router.get("/aggregations/{agg_id}/current")
async def get_aggregation_value(agg_id: str):
    """Get current aggregation value"""
    return stream_aggregator.get_current_value(agg_id)


@router.get("/aggregations/{agg_id}/history")
async def get_aggregation_history(agg_id: str, limit: int = Query(default=10)):
    """Get aggregation history"""
    return {"history": stream_aggregator.get_history(agg_id, limit)}


# Decision Engine Endpoints
@router.post("/decisions/rules")
async def create_decision_rule(
    name: str = Query(...),
    conditions: List[dict] = None,
    action: str = Query(...),
    priority: int = Query(default=5)
):
    """Create decision rule"""
    rule = decision_engine.add_rule(name, conditions or [], action, priority)
    return {"success": True, "rule": rule}


@router.post("/decisions/evaluate")
async def evaluate_decision(data: dict):
    """Evaluate data against decision rules"""
    return decision_engine.evaluate(data)


# Alert Endpoints
@router.post("/alerts/rules")
async def create_alert_rule(
    name: str = Query(...),
    metric: str = Query(...),
    condition: str = Query(...),
    threshold: float = Query(...),
    severity: AlertSeverity = Query(default=AlertSeverity.MEDIUM)
):
    """Create alert rule"""
    rule = alert_manager.create_rule(name, metric, condition, threshold, severity.value)
    return {"success": True, "rule": rule}


@router.get("/alerts")
async def get_active_alerts():
    """Get active alerts"""
    return {"alerts": alert_manager.active_alerts}


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str):
    """Acknowledge an alert"""
    if not alert_manager.acknowledge(alert_id):
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"success": True}


@router.post("/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str):
    """Resolve an alert"""
    if not alert_manager.resolve(alert_id):
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"success": True}


# Time-Series Endpoints
@router.post("/timeseries/{series_id}")
async def ingest_timeseries(series_id: str, value: float = Query(...)):
    """Ingest time-series data point"""
    time_series.ingest(series_id, value)
    return {"success": True}


@router.get("/timeseries/{series_id}")
async def get_timeseries(series_id: str, limit: int = Query(default=100)):
    """Get time-series data"""
    return {"series": time_series.get_series(series_id, limit)}


@router.get("/timeseries/{series_id}/forecast")
async def forecast_timeseries(series_id: str, periods: int = Query(default=10)):
    """Forecast time-series"""
    return {"forecast": time_series.forecast(series_id, periods)}
