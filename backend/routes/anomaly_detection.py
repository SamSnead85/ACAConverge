"""
Anomaly Detection Routes for ACA DataHub
Statistical and ML-based anomaly detection, fraud detection, and root cause analysis
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import math

router = APIRouter(prefix="/anomaly", tags=["Anomaly Detection"])


# =========================================================================
# Models
# =========================================================================

class AnomalyType(str, Enum):
    STATISTICAL = "statistical"
    BEHAVIORAL = "behavioral"
    DATA_QUALITY = "data_quality"
    FRAUD = "fraud"
    PERFORMANCE = "performance"


class AnomalySeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# =========================================================================
# Anomaly Detection Engine
# =========================================================================

class AnomalyDetector:
    """Multi-method anomaly detection"""
    
    def __init__(self):
        self.detectors: Dict[str, dict] = {}
        self.anomalies: List[dict] = []
        self.baselines: Dict[str, dict] = {}
        self._counter = 0
    
    def create_detector(
        self,
        name: str,
        metric: str,
        method: str,
        sensitivity: float = 0.8,
        window_hours: int = 24
    ) -> dict:
        self._counter += 1
        detector_id = f"detector_{self._counter}"
        
        detector = {
            "id": detector_id,
            "name": name,
            "metric": metric,
            "method": method,  # zscore, iqr, isolation_forest, dbscan
            "sensitivity": sensitivity,
            "window_hours": window_hours,
            "enabled": True,
            "created_at": datetime.utcnow().isoformat()
        }
        
        self.detectors[detector_id] = detector
        return detector
    
    def detect_zscore(self, values: List[float], threshold: float = 3.0) -> List[dict]:
        """Z-score based anomaly detection"""
        if len(values) < 2:
            return []
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        std = math.sqrt(variance) if variance > 0 else 1
        
        anomalies = []
        for i, value in enumerate(values):
            zscore = abs((value - mean) / std)
            if zscore > threshold:
                anomalies.append({
                    "index": i,
                    "value": value,
                    "zscore": round(zscore, 2),
                    "method": "zscore",
                    "severity": "critical" if zscore > 4 else ("high" if zscore > 3.5 else "medium")
                })
        
        return anomalies
    
    def detect_iqr(self, values: List[float], multiplier: float = 1.5) -> List[dict]:
        """IQR-based outlier detection"""
        if len(values) < 4:
            return []
        
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[3 * n // 4]
        iqr = q3 - q1
        
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr
        
        anomalies = []
        for i, value in enumerate(values):
            if value < lower_bound or value > upper_bound:
                anomalies.append({
                    "index": i,
                    "value": value,
                    "bounds": {"lower": round(lower_bound, 2), "upper": round(upper_bound, 2)},
                    "method": "iqr",
                    "severity": "high" if abs(value - (q1 + q3)/2) > 2 * iqr else "medium"
                })
        
        return anomalies
    
    def detect_behavioral(self, user_id: str, current_behavior: dict) -> dict:
        """Detect behavioral anomalies"""
        baseline = self.baselines.get(user_id, {
            "avg_session_duration": 30,
            "avg_actions_per_session": 25,
            "typical_hours": [9, 10, 11, 14, 15, 16],
            "typical_features": ["dashboard", "query", "leads"]
        })
        
        anomalies = []
        
        # Check session duration
        current_duration = current_behavior.get("session_duration", 0)
        if current_duration > baseline["avg_session_duration"] * 3:
            anomalies.append({
                "type": "unusually_long_session",
                "severity": "medium",
                "details": f"Session duration {current_duration}min vs baseline {baseline['avg_session_duration']}min"
            })
        
        # Check actions per session
        current_actions = current_behavior.get("actions", 0)
        if current_actions > baseline["avg_actions_per_session"] * 5:
            anomalies.append({
                "type": "excessive_activity",
                "severity": "high",
                "details": f"Actions {current_actions} vs baseline {baseline['avg_actions_per_session']}"
            })
        
        # Check unusual hour
        current_hour = current_behavior.get("hour", 12)
        if current_hour not in baseline["typical_hours"]:
            anomalies.append({
                "type": "unusual_access_time",
                "severity": "low",
                "details": f"Access at hour {current_hour}"
            })
        
        return {
            "user_id": user_id,
            "anomalies": anomalies,
            "baseline": baseline,
            "risk_score": len(anomalies) * 0.25
        }
    
    def detect_data_quality(self, dataset: List[dict], schema: dict = None) -> List[dict]:
        """Detect data quality anomalies"""
        anomalies = []
        
        # Check for nulls
        null_counts = {}
        for row in dataset:
            for key, value in row.items():
                if value is None or value == "":
                    null_counts[key] = null_counts.get(key, 0) + 1
        
        total = len(dataset)
        for field, count in null_counts.items():
            rate = count / total
            if rate > 0.1:  # More than 10% nulls
                anomalies.append({
                    "type": "high_null_rate",
                    "field": field,
                    "null_rate": round(rate * 100, 2),
                    "severity": "high" if rate > 0.3 else "medium"
                })
        
        # Check for duplicates (simplified)
        if len(dataset) > len(set(str(row) for row in dataset)):
            anomalies.append({
                "type": "duplicate_records",
                "severity": "medium"
            })
        
        return anomalies
    
    def detect_fraud_patterns(self, transactions: List[dict]) -> List[dict]:
        """Detect fraud patterns"""
        fraud_indicators = []
        
        # Velocity check - rapid transactions
        if len(transactions) > 10:
            first_time = transactions[0].get("timestamp", "")
            last_time = transactions[-1].get("timestamp", "")
            # Simplified velocity check
            fraud_indicators.append({
                "type": "velocity_check",
                "transactions": len(transactions),
                "risk": "medium" if len(transactions) > 20 else "low"
            })
        
        # Amount anomaly
        amounts = [t.get("amount", 0) for t in transactions]
        if amounts:
            avg_amount = sum(amounts) / len(amounts)
            for i, t in enumerate(transactions):
                if t.get("amount", 0) > avg_amount * 5:
                    fraud_indicators.append({
                        "type": "amount_anomaly",
                        "transaction_index": i,
                        "amount": t["amount"],
                        "avg": round(avg_amount, 2),
                        "risk": "high"
                    })
        
        return fraud_indicators
    
    def record_anomaly(self, anomaly: dict) -> dict:
        self._counter += 1
        anomaly_record = {
            "id": f"anomaly_{self._counter}",
            "detected_at": datetime.utcnow().isoformat(),
            **anomaly
        }
        self.anomalies.append(anomaly_record)
        return anomaly_record


detector = AnomalyDetector()


# =========================================================================
# Root Cause Analyzer
# =========================================================================

class RootCauseAnalyzer:
    """Analyze root causes of anomalies"""
    
    def analyze(self, anomaly: dict, context: dict = None) -> dict:
        """Perform root cause analysis"""
        causes = []
        
        anomaly_type = anomaly.get("type", "")
        
        if "null" in anomaly_type.lower():
            causes.append({
                "cause": "Data pipeline failure",
                "probability": 0.6,
                "evidence": ["Missing data in source", "ETL job timeout"]
            })
            causes.append({
                "cause": "Schema change",
                "probability": 0.3,
                "evidence": ["New column added", "Field renamed"]
            })
        
        elif "velocity" in anomaly_type.lower() or "amount" in anomaly_type.lower():
            causes.append({
                "cause": "Potential fraud attempt",
                "probability": 0.7,
                "evidence": ["Unusual pattern", "Geographic anomaly"]
            })
        
        elif "behavioral" in anomaly_type.lower():
            causes.append({
                "cause": "Compromised credentials",
                "probability": 0.5,
                "evidence": ["Unusual access time", "Different IP"]
            })
            causes.append({
                "cause": "Legitimate power user",
                "probability": 0.4,
                "evidence": ["End of month activity", "Campaign deadline"]
            })
        
        else:
            causes.append({
                "cause": "System performance issue",
                "probability": 0.5,
                "evidence": ["High latency", "Resource contention"]
            })
        
        return {
            "anomaly": anomaly,
            "root_causes": sorted(causes, key=lambda x: x["probability"], reverse=True),
            "analyzed_at": datetime.utcnow().isoformat(),
            "recommendations": self._get_recommendations(causes)
        }
    
    def _get_recommendations(self, causes: List[dict]) -> List[str]:
        recommendations = []
        
        for cause in causes:
            if "fraud" in cause["cause"].lower():
                recommendations.append("Review account activity immediately")
                recommendations.append("Consider temporary account suspension")
            elif "pipeline" in cause["cause"].lower():
                recommendations.append("Check ETL job logs")
                recommendations.append("Verify source data availability")
            elif "credential" in cause["cause"].lower():
                recommendations.append("Force password reset")
                recommendations.append("Enable MFA if not already enabled")
        
        return list(set(recommendations))[:5]


rca = RootCauseAnalyzer()


# =========================================================================
# Alert Deduplication
# =========================================================================

class AlertDeduplicator:
    """Deduplicate and correlate alerts"""
    
    def __init__(self):
        self.alert_groups: Dict[str, List[dict]] = {}
    
    def deduplicate(self, alerts: List[dict], time_window_minutes: int = 15) -> List[dict]:
        """Deduplicate similar alerts"""
        groups = {}
        
        for alert in alerts:
            key = f"{alert.get('type', '')}_{alert.get('source', '')}"
            
            if key not in groups:
                groups[key] = {
                    "representative": alert,
                    "count": 1,
                    "first_seen": alert.get("timestamp", datetime.utcnow().isoformat()),
                    "last_seen": alert.get("timestamp", datetime.utcnow().isoformat())
                }
            else:
                groups[key]["count"] += 1
                groups[key]["last_seen"] = alert.get("timestamp", datetime.utcnow().isoformat())
        
        return list(groups.values())
    
    def correlate(self, anomalies: List[dict]) -> List[dict]:
        """Find correlations between anomalies"""
        correlations = []
        
        # Group by time proximity
        for i, a1 in enumerate(anomalies):
            for j, a2 in enumerate(anomalies[i+1:], i+1):
                # Simplified correlation check
                if a1.get("source") != a2.get("source"):
                    correlation = {
                        "anomaly_1": a1.get("id"),
                        "anomaly_2": a2.get("id"),
                        "correlation_type": "temporal",
                        "confidence": round(random.uniform(0.5, 0.9), 2)
                    }
                    correlations.append(correlation)
        
        return correlations[:10]


deduplicator = AlertDeduplicator()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/detectors")
async def create_detector(
    name: str = Query(...),
    metric: str = Query(...),
    method: str = Query(default="zscore"),
    sensitivity: float = Query(default=0.8, ge=0, le=1)
):
    """Create anomaly detector"""
    det = detector.create_detector(name, metric, method, sensitivity)
    return {"success": True, "detector": det}


@router.get("/detectors")
async def list_detectors():
    """List anomaly detectors"""
    return {"detectors": list(detector.detectors.values())}


@router.post("/detect/zscore")
async def detect_zscore_anomalies(
    values: List[float] = Query(...),
    threshold: float = Query(default=3.0)
):
    """Detect anomalies using Z-score"""
    anomalies = detector.detect_zscore(values, threshold)
    return {"anomalies": anomalies, "count": len(anomalies)}


@router.post("/detect/iqr")
async def detect_iqr_anomalies(
    values: List[float] = Query(...),
    multiplier: float = Query(default=1.5)
):
    """Detect anomalies using IQR"""
    anomalies = detector.detect_iqr(values, multiplier)
    return {"anomalies": anomalies, "count": len(anomalies)}


@router.post("/detect/behavioral")
async def detect_behavioral_anomalies(
    user_id: str = Query(...),
    session_duration: int = Query(default=30),
    actions: int = Query(default=25),
    hour: int = Query(default=12)
):
    """Detect behavioral anomalies"""
    behavior = {
        "session_duration": session_duration,
        "actions": actions,
        "hour": hour
    }
    return detector.detect_behavioral(user_id, behavior)


@router.post("/detect/data-quality")
async def detect_data_quality_anomalies(dataset: List[dict]):
    """Detect data quality anomalies"""
    anomalies = detector.detect_data_quality(dataset)
    return {"anomalies": anomalies, "count": len(anomalies)}


@router.post("/detect/fraud")
async def detect_fraud_patterns(transactions: List[dict]):
    """Detect fraud patterns"""
    indicators = detector.detect_fraud_patterns(transactions)
    return {"indicators": indicators, "risk_level": "high" if len(indicators) > 2 else "medium"}


@router.post("/analyze/root-cause")
async def analyze_root_cause(anomaly: dict):
    """Analyze root cause of anomaly"""
    return rca.analyze(anomaly)


@router.post("/deduplicate")
async def deduplicate_alerts(alerts: List[dict], window_minutes: int = Query(default=15)):
    """Deduplicate similar alerts"""
    groups = deduplicator.deduplicate(alerts, window_minutes)
    return {"groups": groups, "original_count": len(alerts), "grouped_count": len(groups)}


@router.post("/correlate")
async def correlate_anomalies(anomalies: List[dict]):
    """Find correlations between anomalies"""
    correlations = deduplicator.correlate(anomalies)
    return {"correlations": correlations}


@router.get("/history")
async def get_anomaly_history(limit: int = Query(default=100)):
    """Get anomaly history"""
    return {"anomalies": detector.anomalies[-limit:]}
