"""
Data Observability Routes for ACA DataHub
Data quality monitoring, schema drift, freshness alerts, and automated remediation
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random

router = APIRouter(prefix="/observability", tags=["Data Observability"])


# =========================================================================
# Models
# =========================================================================

class QualityDimension(str, Enum):
    FRESHNESS = "freshness"
    VOLUME = "volume"
    SCHEMA = "schema"
    DISTRIBUTION = "distribution"
    UNIQUENESS = "uniqueness"
    COMPLETENESS = "completeness"


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# =========================================================================
# Data Quality Monitor
# =========================================================================

class DataQualityMonitor:
    """Monitor data quality across datasets"""
    
    def __init__(self):
        self.monitors: Dict[str, dict] = {}
        self.checks: List[dict] = []
        self.alerts: List[dict] = []
        self._counter = 0
        self._init_monitors()
    
    def _init_monitors(self):
        """Initialize default monitors"""
        tables = ["leads", "populations", "campaigns", "users"]
        
        for table in tables:
            monitor_id = f"mon_{table}"
            self.monitors[monitor_id] = {
                "id": monitor_id,
                "table": table,
                "enabled": True,
                "dimensions": [d.value for d in QualityDimension],
                "schedule": "*/15 * * * *",  # Every 15 minutes
                "last_run": datetime.utcnow().isoformat(),
                "status": "healthy"
            }
    
    def create_monitor(
        self,
        table: str,
        dimensions: List[str],
        thresholds: dict = None
    ) -> dict:
        self._counter += 1
        monitor_id = f"mon_{self._counter}"
        
        monitor = {
            "id": monitor_id,
            "table": table,
            "enabled": True,
            "dimensions": dimensions,
            "thresholds": thresholds or {},
            "schedule": "*/15 * * * *",
            "created_at": datetime.utcnow().isoformat(),
            "last_run": None,
            "status": "pending"
        }
        
        self.monitors[monitor_id] = monitor
        return monitor
    
    def run_check(self, monitor_id: str) -> dict:
        """Run quality check for a monitor"""
        if monitor_id not in self.monitors:
            raise ValueError("Monitor not found")
        
        monitor = self.monitors[monitor_id]
        self._counter += 1
        check_id = f"check_{self._counter}"
        
        results = {}
        issues = []
        
        for dimension in monitor["dimensions"]:
            check_result = self._check_dimension(dimension, monitor["table"])
            results[dimension] = check_result
            
            if not check_result["passed"]:
                issues.append({
                    "dimension": dimension,
                    "severity": check_result["severity"],
                    "message": check_result["message"]
                })
        
        check = {
            "id": check_id,
            "monitor_id": monitor_id,
            "table": monitor["table"],
            "timestamp": datetime.utcnow().isoformat(),
            "results": results,
            "issues": issues,
            "passed": len(issues) == 0,
            "duration_ms": random.randint(100, 2000)
        }
        
        self.checks.append(check)
        monitor["last_run"] = check["timestamp"]
        monitor["status"] = "healthy" if check["passed"] else "degraded"
        
        # Create alerts for issues
        for issue in issues:
            self._create_alert(monitor_id, issue)
        
        return check
    
    def _check_dimension(self, dimension: str, table: str) -> dict:
        """Check a specific quality dimension"""
        passed = random.random() > 0.15  # 85% pass rate
        
        if dimension == "freshness":
            hours_old = random.randint(0, 48)
            threshold = 24
            return {
                "dimension": dimension,
                "value": hours_old,
                "threshold": threshold,
                "passed": hours_old <= threshold,
                "severity": "error" if hours_old > threshold else "info",
                "message": f"Data is {hours_old} hours old" if hours_old > threshold else "Data is fresh"
            }
        
        elif dimension == "volume":
            expected = random.randint(1000, 10000)
            actual = expected + random.randint(-500, 500)
            deviation = abs(actual - expected) / expected * 100
            return {
                "dimension": dimension,
                "expected": expected,
                "actual": actual,
                "deviation_percent": round(deviation, 2),
                "passed": deviation < 20,
                "severity": "warning" if deviation >= 20 else "info",
                "message": f"Volume deviation: {deviation:.1f}%" if deviation >= 20 else "Volume normal"
            }
        
        elif dimension == "completeness":
            null_rate = round(random.uniform(0, 0.15), 3)
            threshold = 0.05
            return {
                "dimension": dimension,
                "null_rate": null_rate,
                "threshold": threshold,
                "passed": null_rate <= threshold,
                "severity": "warning" if null_rate > threshold else "info",
                "message": f"Null rate: {null_rate*100:.1f}%"
            }
        
        else:
            return {
                "dimension": dimension,
                "passed": passed,
                "severity": "info" if passed else "warning",
                "message": f"{dimension} check {'passed' if passed else 'failed'}"
            }
    
    def _create_alert(self, monitor_id: str, issue: dict):
        self._counter += 1
        alert = {
            "id": f"alert_{self._counter}",
            "monitor_id": monitor_id,
            "dimension": issue["dimension"],
            "severity": issue["severity"],
            "message": issue["message"],
            "created_at": datetime.utcnow().isoformat(),
            "acknowledged": False,
            "resolved": False
        }
        self.alerts.append(alert)


quality_monitor = DataQualityMonitor()


# =========================================================================
# Schema Drift Detector
# =========================================================================

class SchemaDriftDetector:
    """Detect and track schema changes"""
    
    def __init__(self):
        self.schemas: Dict[str, dict] = {}
        self.changes: List[dict] = []
        self._counter = 0
    
    def register_schema(self, table: str, columns: List[dict]) -> dict:
        """Register current schema for a table"""
        schema = {
            "table": table,
            "columns": columns,
            "registered_at": datetime.utcnow().isoformat(),
            "version": self.schemas.get(table, {}).get("version", 0) + 1
        }
        self.schemas[table] = schema
        return schema
    
    def detect_drift(self, table: str, new_columns: List[dict]) -> dict:
        """Detect schema drift from registered schema"""
        if table not in self.schemas:
            return {"drifted": False, "message": "No baseline schema registered"}
        
        old_schema = self.schemas[table]
        old_cols = {c["name"]: c for c in old_schema["columns"]}
        new_cols = {c["name"]: c for c in new_columns}
        
        changes = []
        
        # Detect added columns
        for name in new_cols:
            if name not in old_cols:
                changes.append({
                    "type": "column_added",
                    "column": name,
                    "details": new_cols[name],
                    "breaking": False
                })
        
        # Detect removed columns
        for name in old_cols:
            if name not in new_cols:
                changes.append({
                    "type": "column_removed",
                    "column": name,
                    "breaking": True
                })
        
        # Detect type changes
        for name in old_cols:
            if name in new_cols:
                if old_cols[name].get("type") != new_cols[name].get("type"):
                    changes.append({
                        "type": "type_changed",
                        "column": name,
                        "old_type": old_cols[name].get("type"),
                        "new_type": new_cols[name].get("type"),
                        "breaking": True
                    })
        
        self._counter += 1
        drift_record = {
            "id": f"drift_{self._counter}",
            "table": table,
            "detected_at": datetime.utcnow().isoformat(),
            "changes": changes,
            "drifted": len(changes) > 0,
            "has_breaking_changes": any(c.get("breaking") for c in changes)
        }
        
        if changes:
            self.changes.append(drift_record)
        
        return drift_record


schema_detector = SchemaDriftDetector()


# =========================================================================
# Data Profiler
# =========================================================================

class DataProfiler:
    """Profile data distributions and statistics"""
    
    def __init__(self):
        self.profiles: Dict[str, dict] = {}
    
    def profile_table(self, table: str, sample_size: int = 1000) -> dict:
        """Generate profile for a table"""
        columns = ["id", "name", "email", "score", "state", "created_at"]
        
        column_profiles = []
        for col in columns:
            column_profiles.append(self._profile_column(col))
        
        profile = {
            "table": table,
            "row_count": random.randint(10000, 100000),
            "sample_size": sample_size,
            "columns": column_profiles,
            "profiled_at": datetime.utcnow().isoformat()
        }
        
        self.profiles[table] = profile
        return profile
    
    def _profile_column(self, column: str) -> dict:
        """Profile a single column"""
        if column in ["score", "value", "count"]:
            return {
                "name": column,
                "type": "numeric",
                "null_count": random.randint(0, 100),
                "null_percent": round(random.uniform(0, 5), 2),
                "distinct_count": random.randint(50, 100),
                "min": random.randint(0, 50),
                "max": random.randint(50, 100),
                "mean": round(random.uniform(40, 60), 2),
                "median": random.randint(40, 60),
                "std": round(random.uniform(10, 20), 2)
            }
        elif column in ["email", "name"]:
            return {
                "name": column,
                "type": "string",
                "null_count": random.randint(0, 50),
                "null_percent": round(random.uniform(0, 2), 2),
                "distinct_count": random.randint(5000, 10000),
                "avg_length": round(random.uniform(15, 30), 1),
                "min_length": random.randint(5, 10),
                "max_length": random.randint(40, 100)
            }
        elif column in ["state", "status"]:
            return {
                "name": column,
                "type": "categorical",
                "null_count": random.randint(0, 10),
                "null_percent": round(random.uniform(0, 1), 2),
                "distinct_count": random.randint(5, 50),
                "top_values": [
                    {"value": "GA", "count": random.randint(100, 500)},
                    {"value": "FL", "count": random.randint(100, 500)},
                    {"value": "TX", "count": random.randint(100, 500)}
                ]
            }
        else:
            return {
                "name": column,
                "type": "unknown",
                "null_count": random.randint(0, 100),
                "distinct_count": random.randint(100, 1000)
            }


profiler = DataProfiler()


# =========================================================================
# Remediation Engine
# =========================================================================

class RemediationEngine:
    """Automated remediation for data issues"""
    
    def __init__(self):
        self.playbooks: Dict[str, dict] = {}
        self.executions: List[dict] = []
        self._counter = 0
        self._init_playbooks()
    
    def _init_playbooks(self):
        self.playbooks = {
            "stale_data": {
                "id": "stale_data",
                "name": "Stale Data Remediation",
                "trigger": {"dimension": "freshness", "threshold": 24},
                "actions": [
                    {"type": "alert", "target": "data_team"},
                    {"type": "trigger_pipeline", "pipeline": "refresh_pipeline"}
                ]
            },
            "high_null_rate": {
                "id": "high_null_rate",
                "name": "High Null Rate Fix",
                "trigger": {"dimension": "completeness", "threshold": 0.1},
                "actions": [
                    {"type": "alert", "target": "data_quality_team"},
                    {"type": "quarantine", "table": "affected_table"}
                ]
            }
        }
    
    def execute_playbook(self, playbook_id: str, context: dict = None) -> dict:
        if playbook_id not in self.playbooks:
            raise ValueError("Playbook not found")
        
        playbook = self.playbooks[playbook_id]
        self._counter += 1
        
        results = []
        for action in playbook["actions"]:
            results.append({
                "action": action["type"],
                "status": "completed",
                "timestamp": datetime.utcnow().isoformat()
            })
        
        execution = {
            "id": f"remediation_{self._counter}",
            "playbook_id": playbook_id,
            "playbook_name": playbook["name"],
            "context": context or {},
            "results": results,
            "executed_at": datetime.utcnow().isoformat()
        }
        
        self.executions.append(execution)
        return execution


remediation = RemediationEngine()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/monitors")
async def list_monitors():
    """List data quality monitors"""
    return {"monitors": list(quality_monitor.monitors.values())}


@router.post("/monitors")
async def create_monitor(
    table: str = Query(...),
    dimensions: List[QualityDimension] = Query(default=None)
):
    """Create data quality monitor"""
    dims = [d.value for d in dimensions] if dimensions else [d.value for d in QualityDimension]
    return quality_monitor.create_monitor(table, dims)


@router.post("/monitors/{monitor_id}/check")
async def run_quality_check(monitor_id: str):
    """Run quality check for monitor"""
    try:
        return quality_monitor.run_check(monitor_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/checks")
async def list_checks(limit: int = Query(default=50)):
    """List quality check results"""
    return {"checks": quality_monitor.checks[-limit:]}


@router.get("/alerts")
async def list_alerts(acknowledged: Optional[bool] = Query(default=None)):
    """List quality alerts"""
    alerts = quality_monitor.alerts
    if acknowledged is not None:
        alerts = [a for a in alerts if a["acknowledged"] == acknowledged]
    return {"alerts": alerts}


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str):
    """Acknowledge an alert"""
    for alert in quality_monitor.alerts:
        if alert["id"] == alert_id:
            alert["acknowledged"] = True
            return {"success": True}
    raise HTTPException(status_code=404, detail="Alert not found")


@router.post("/schema/register")
async def register_schema(table: str = Query(...), columns: List[dict] = None):
    """Register schema for drift detection"""
    return schema_detector.register_schema(table, columns or [])


@router.post("/schema/detect-drift")
async def detect_schema_drift(table: str = Query(...), columns: List[dict] = None):
    """Detect schema drift"""
    return schema_detector.detect_drift(table, columns or [])


@router.get("/schema/changes")
async def list_schema_changes():
    """List detected schema changes"""
    return {"changes": schema_detector.changes}


@router.post("/profile/{table}")
async def profile_table(table: str, sample_size: int = Query(default=1000)):
    """Profile table data"""
    return profiler.profile_table(table, sample_size)


@router.get("/profiles")
async def list_profiles():
    """List table profiles"""
    return {"profiles": list(profiler.profiles.values())}


@router.get("/remediation/playbooks")
async def list_playbooks():
    """List remediation playbooks"""
    return {"playbooks": list(remediation.playbooks.values())}


@router.post("/remediation/execute/{playbook_id}")
async def execute_remediation(playbook_id: str):
    """Execute remediation playbook"""
    try:
        return remediation.execute_playbook(playbook_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/dashboard")
async def get_observability_dashboard():
    """Get observability dashboard summary"""
    return {
        "monitors": len(quality_monitor.monitors),
        "healthy_monitors": sum(1 for m in quality_monitor.monitors.values() if m["status"] == "healthy"),
        "active_alerts": sum(1 for a in quality_monitor.alerts if not a["acknowledged"]),
        "recent_checks": len(quality_monitor.checks),
        "schema_changes_detected": len(schema_detector.changes),
        "last_updated": datetime.utcnow().isoformat()
    }
