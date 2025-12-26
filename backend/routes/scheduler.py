"""
Scheduler API Routes
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

from services.scheduler import (
    scheduler, ScheduledJob, JobType, ScheduleFrequency, ScheduleStatus
)

router = APIRouter()


class CreateScheduleRequest(BaseModel):
    name: str
    job_type: str  # report, message, refresh, export
    frequency: str  # once, hourly, daily, weekly, monthly
    config: Dict[str, Any]  # Job-specific config
    start_time: Optional[str] = None  # ISO datetime string


@router.post("/schedule")
async def create_scheduled_job(request: CreateScheduleRequest, job_id: str = Query(...)):
    """Create a new scheduled job"""
    try:
        job_type = JobType(request.job_type)
        frequency = ScheduleFrequency(request.frequency)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job_type or frequency")
    
    start_time = None
    if request.start_time:
        try:
            start_time = datetime.fromisoformat(request.start_time)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid start_time format")
    
    scheduled = scheduler.schedule_job(
        name=request.name,
        job_type=job_type,
        frequency=frequency,
        job_id=job_id,
        config=request.config,
        start_time=start_time
    )
    
    return {
        "message": "Job scheduled",
        "scheduled_job": scheduled.to_dict()
    }


@router.get("/schedules")
async def list_scheduled_jobs(job_id: str = Query(None)):
    """List scheduled jobs"""
    jobs = scheduler.list_jobs(job_id)
    return {
        "count": len(jobs),
        "scheduled_jobs": [j.to_dict() for j in jobs]
    }


@router.get("/schedule/{schedule_id}")
async def get_scheduled_job(schedule_id: str):
    """Get a specific scheduled job"""
    job = scheduler.get_job(schedule_id)
    if not job:
        raise HTTPException(status_code=404, detail="Scheduled job not found")
    return {"scheduled_job": job.to_dict()}


@router.post("/schedule/{schedule_id}/pause")
async def pause_scheduled_job(schedule_id: str):
    """Pause a scheduled job"""
    success = scheduler.pause_job(schedule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Scheduled job not found")
    return {"message": "Job paused"}


@router.post("/schedule/{schedule_id}/resume")
async def resume_scheduled_job(schedule_id: str):
    """Resume a paused job"""
    success = scheduler.resume_job(schedule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Scheduled job not found")
    return {"message": "Job resumed"}


@router.delete("/schedule/{schedule_id}")
async def delete_scheduled_job(schedule_id: str):
    """Delete a scheduled job"""
    success = scheduler.delete_job(schedule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Scheduled job not found")
    return {"message": "Job deleted"}


@router.get("/schedules/upcoming")
async def get_upcoming_jobs(hours: int = Query(24, ge=1, le=168)):
    """Get jobs scheduled to run in the next N hours"""
    jobs = scheduler.get_upcoming_jobs(hours)
    return {
        "hours": hours,
        "count": len(jobs),
        "upcoming_jobs": [j.to_dict() for j in jobs]
    }


@router.get("/schedules/due")
async def get_due_jobs():
    """Get jobs that are due to run now"""
    jobs = scheduler.get_due_jobs()
    return {
        "count": len(jobs),
        "due_jobs": [j.to_dict() for j in jobs]
    }
