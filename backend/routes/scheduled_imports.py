"""
Scheduled Imports & Data Pipeline Routes for ACA DataHub
Cron-based scheduling, data quality scoring, and lineage tracking
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import re

router = APIRouter(prefix="/imports", tags=["Scheduled Imports"])


# =========================================================================
# Models
# =========================================================================

class ScheduleFrequency(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM = "custom"


class ScheduleRequest(BaseModel):
    name: str
    source_url: Optional[str] = None
    source_path: Optional[str] = None
    frequency: ScheduleFrequency
    cron_expression: Optional[str] = None  # For custom frequency
    next_run: Optional[str] = None
    is_delta: bool = False  # Incremental import
    delta_column: Optional[str] = None  # Column to detect changes
    enabled: bool = True


class ScheduleResponse(BaseModel):
    id: str
    name: str
    frequency: str
    cron_expression: Optional[str]
    next_run: str
    last_run: Optional[str]
    last_status: Optional[str]
    enabled: bool
    created_at: str


# =========================================================================
# Schedule Store (In-memory for demo)
# =========================================================================

class ScheduleStore:
    """Stores scheduled import configurations"""
    
    def __init__(self):
        self.schedules: Dict[str, dict] = {}
        self.run_history: Dict[str, List[dict]] = {}
        self._counter = 0
    
    def create(self, schedule: dict) -> dict:
        self._counter += 1
        schedule_id = f"sched_{self._counter}"
        schedule["id"] = schedule_id
        schedule["created_at"] = datetime.utcnow().isoformat()
        schedule["last_run"] = None
        schedule["last_status"] = None
        
        # Calculate next run
        if not schedule.get("next_run"):
            schedule["next_run"] = self._calculate_next_run(
                schedule["frequency"],
                schedule.get("cron_expression")
            )
        
        self.schedules[schedule_id] = schedule
        return schedule
    
    def get(self, schedule_id: str) -> Optional[dict]:
        return self.schedules.get(schedule_id)
    
    def list(self, enabled_only: bool = False) -> List[dict]:
        schedules = list(self.schedules.values())
        if enabled_only:
            schedules = [s for s in schedules if s.get("enabled", True)]
        return sorted(schedules, key=lambda x: x.get("created_at", ""), reverse=True)
    
    def update(self, schedule_id: str, updates: dict) -> Optional[dict]:
        if schedule_id not in self.schedules:
            return None
        self.schedules[schedule_id].update(updates)
        return self.schedules[schedule_id]
    
    def delete(self, schedule_id: str) -> bool:
        if schedule_id in self.schedules:
            del self.schedules[schedule_id]
            return True
        return False
    
    def record_run(self, schedule_id: str, status: str, details: dict = None):
        if schedule_id not in self.run_history:
            self.run_history[schedule_id] = []
        
        run_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "status": status,
            "details": details or {}
        }
        
        self.run_history[schedule_id].insert(0, run_record)
        self.run_history[schedule_id] = self.run_history[schedule_id][:100]
        
        # Update schedule
        if schedule_id in self.schedules:
            self.schedules[schedule_id]["last_run"] = run_record["timestamp"]
            self.schedules[schedule_id]["last_status"] = status
            self.schedules[schedule_id]["next_run"] = self._calculate_next_run(
                self.schedules[schedule_id]["frequency"],
                self.schedules[schedule_id].get("cron_expression")
            )
    
    def get_history(self, schedule_id: str) -> List[dict]:
        return self.run_history.get(schedule_id, [])
    
    def _calculate_next_run(self, frequency: str, cron_expr: str = None) -> str:
        now = datetime.utcnow()
        
        if frequency == "hourly":
            next_run = now + timedelta(hours=1)
        elif frequency == "daily":
            next_run = now + timedelta(days=1)
        elif frequency == "weekly":
            next_run = now + timedelta(weeks=1)
        elif frequency == "monthly":
            next_run = now + timedelta(days=30)
        else:
            # Custom - default to daily
            next_run = now + timedelta(days=1)
        
        return next_run.isoformat()


schedule_store = ScheduleStore()


# =========================================================================
# Data Quality Engine
# =========================================================================

class DataQualityEngine:
    """Analyzes data quality and generates scores"""
    
    def analyze(self, data: List[dict], schema: dict = None) -> dict:
        """Analyze data quality and return metrics"""
        if not data:
            return {"overall_score": 0, "columns": {}}
        
        column_metrics = {}
        total_rows = len(data)
        
        # Get all columns
        columns = set()
        for row in data:
            columns.update(row.keys())
        
        for column in columns:
            values = [row.get(column) for row in data]
            null_count = sum(1 for v in values if v is None or v == "" or v == "null")
            non_null_values = [v for v in values if v is not None and v != "" and v != "null"]
            
            metrics = {
                "completeness": round((total_rows - null_count) / total_rows * 100, 1),
                "null_count": null_count,
                "unique_count": len(set(str(v) for v in non_null_values)),
                "total_count": total_rows
            }
            
            # Detect data type issues
            if non_null_values:
                type_counts = {"string": 0, "number": 0, "date": 0}
                for v in non_null_values:
                    if isinstance(v, (int, float)):
                        type_counts["number"] += 1
                    elif self._is_date(str(v)):
                        type_counts["date"] += 1
                    else:
                        type_counts["string"] += 1
                
                dominant_type = max(type_counts, key=type_counts.get)
                metrics["detected_type"] = dominant_type
                metrics["consistency"] = round(type_counts[dominant_type] / len(non_null_values) * 100, 1)
                
                # Check for duplicates
                if metrics["unique_count"] < len(non_null_values):
                    metrics["duplicate_rate"] = round(
                        (len(non_null_values) - metrics["unique_count"]) / len(non_null_values) * 100, 1
                    )
                else:
                    metrics["duplicate_rate"] = 0
            
            # Calculate column quality score
            completeness_weight = 0.4
            consistency_weight = 0.3
            uniqueness_weight = 0.3
            
            uniqueness_score = min(100, (metrics["unique_count"] / max(1, len(non_null_values))) * 100)
            
            metrics["quality_score"] = round(
                metrics["completeness"] * completeness_weight +
                metrics.get("consistency", 100) * consistency_weight +
                uniqueness_score * uniqueness_weight,
                1
            )
            
            column_metrics[column] = metrics
        
        # Calculate overall score
        overall_score = round(
            sum(m["quality_score"] for m in column_metrics.values()) / max(1, len(column_metrics)),
            1
        )
        
        return {
            "overall_score": overall_score,
            "total_rows": total_rows,
            "total_columns": len(columns),
            "columns": column_metrics,
            "analyzed_at": datetime.utcnow().isoformat()
        }
    
    def _is_date(self, value: str) -> bool:
        """Check if string looks like a date"""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{2}-\d{2}-\d{4}'
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)


data_quality_engine = DataQualityEngine()


# =========================================================================
# Data Lineage Tracker
# =========================================================================

class DataLineageTracker:
    """Tracks data transformations and lineage"""
    
    def __init__(self):
        # job_id -> lineage info
        self.lineage: Dict[str, dict] = {}
    
    def record_import(self, job_id: str, source: dict):
        """Record a data import"""
        self.lineage[job_id] = {
            "job_id": job_id,
            "source": source,
            "created_at": datetime.utcnow().isoformat(),
            "transformations": [],
            "derived_from": [],
            "schema_versions": []
        }
    
    def record_transformation(self, job_id: str, transformation: dict):
        """Record a transformation applied to data"""
        if job_id in self.lineage:
            transformation["timestamp"] = datetime.utcnow().isoformat()
            self.lineage[job_id]["transformations"].append(transformation)
    
    def record_schema_change(self, job_id: str, old_schema: dict, new_schema: dict):
        """Record a schema change"""
        if job_id in self.lineage:
            self.lineage[job_id]["schema_versions"].append({
                "old_schema": old_schema,
                "new_schema": new_schema,
                "changed_at": datetime.utcnow().isoformat()
            })
    
    def get_lineage(self, job_id: str) -> Optional[dict]:
        return self.lineage.get(job_id)
    
    def get_impact_analysis(self, job_id: str) -> dict:
        """Analyze what would be affected by changes to this data"""
        lineage = self.lineage.get(job_id)
        if not lineage:
            return {"affected_items": []}
        
        # In production, would track populations, reports, etc. using this data
        return {
            "source_job": job_id,
            "affected_populations": [],
            "affected_reports": [],
            "affected_campaigns": [],
            "last_updated": lineage["created_at"]
        }


lineage_tracker = DataLineageTracker()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/schedule", response_model=dict)
async def create_schedule(schedule: ScheduleRequest):
    """Create a new scheduled import"""
    result = schedule_store.create(schedule.dict())
    return {"success": True, "schedule": result}


@router.get("/schedules")
async def list_schedules(enabled_only: bool = Query(default=False)):
    """List all scheduled imports"""
    schedules = schedule_store.list(enabled_only)
    return {"schedules": schedules}


@router.get("/schedule/{schedule_id}")
async def get_schedule(schedule_id: str):
    """Get a specific schedule"""
    schedule = schedule_store.get(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return schedule


@router.put("/schedule/{schedule_id}")
async def update_schedule(schedule_id: str, updates: dict):
    """Update a schedule"""
    result = schedule_store.update(schedule_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True, "schedule": result}


@router.delete("/schedule/{schedule_id}")
async def delete_schedule(schedule_id: str):
    """Delete a schedule"""
    if not schedule_store.delete(schedule_id):
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True}


@router.post("/schedule/{schedule_id}/run-now")
async def run_schedule_now(schedule_id: str, background_tasks: BackgroundTasks):
    """Manually trigger a scheduled import"""
    schedule = schedule_store.get(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    
    # In production, would queue actual import job
    schedule_store.record_run(schedule_id, "triggered", {"manual": True})
    
    return {"success": True, "message": "Import triggered"}


@router.get("/schedule/{schedule_id}/history")
async def get_schedule_history(schedule_id: str):
    """Get run history for a schedule"""
    history = schedule_store.get_history(schedule_id)
    return {"schedule_id": schedule_id, "history": history}


# Data Quality Endpoints
@router.post("/quality/analyze")
async def analyze_data_quality(job_id: str = Query(...)):
    """Analyze data quality for a job"""
    # In production, would fetch actual data from the job
    # For demo, return sample metrics
    sample_data = [
        {"name": "John Doe", "email": "john@example.com", "age": 35},
        {"name": "Jane Smith", "email": "", "age": 28},
        {"name": None, "email": "test@test.com", "age": None}
    ]
    
    result = data_quality_engine.analyze(sample_data)
    result["job_id"] = job_id
    
    return result


@router.get("/lineage/{job_id}")
async def get_data_lineage(job_id: str):
    """Get lineage information for a job"""
    lineage = lineage_tracker.get_lineage(job_id)
    if not lineage:
        return {"job_id": job_id, "lineage": None, "message": "No lineage data found"}
    return lineage


@router.get("/lineage/{job_id}/impact")
async def get_impact_analysis(job_id: str):
    """Analyze impact of changes to this data"""
    return lineage_tracker.get_impact_analysis(job_id)
