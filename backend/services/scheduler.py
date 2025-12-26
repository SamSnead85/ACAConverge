"""
Scheduled Jobs Service
Schedule and manage recurring jobs for reports and messages
"""

import json
import os
import uuid
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import threading


class ScheduleFrequency(str, Enum):
    ONCE = "once"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class JobType(str, Enum):
    REPORT = "report"
    MESSAGE = "message"
    REFRESH = "refresh"
    EXPORT = "export"


class ScheduleStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ScheduledJob:
    """Represents a scheduled job"""
    id: str
    name: str
    job_type: JobType
    frequency: ScheduleFrequency
    job_id: str  # The conversion job ID
    config: Dict  # Job-specific configuration
    next_run: str
    last_run: Optional[str] = None
    status: ScheduleStatus = ScheduleStatus.ACTIVE
    run_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict:
        result = asdict(self)
        result['job_type'] = self.job_type.value
        result['frequency'] = self.frequency.value
        result['status'] = self.status.value
        return result


@dataclass
class JobRun:
    """Record of a job execution"""
    id: str
    scheduled_job_id: str
    started_at: str
    completed_at: Optional[str] = None
    success: bool = False
    result: Optional[Dict] = None
    error: Optional[str] = None


class SchedulerService:
    """Manage scheduled jobs"""
    
    def __init__(self):
        self.jobs: Dict[str, ScheduledJob] = {}
        self.run_history: Dict[str, List[JobRun]] = {}
        self._storage_file = "databases/scheduled_jobs.json"
        self._load_jobs()
        self._running = False
        self._thread = None
    
    def _load_jobs(self):
        """Load jobs from storage"""
        if os.path.exists(self._storage_file):
            try:
                with open(self._storage_file, 'r') as f:
                    data = json.load(f)
                    for job_data in data.get('jobs', []):
                        job_data['job_type'] = JobType(job_data['job_type'])
                        job_data['frequency'] = ScheduleFrequency(job_data['frequency'])
                        job_data['status'] = ScheduleStatus(job_data['status'])
                        job = ScheduledJob(**job_data)
                        self.jobs[job.id] = job
            except Exception as e:
                print(f"Error loading scheduled jobs: {e}")
    
    def _save_jobs(self):
        """Save jobs to storage"""
        os.makedirs("databases", exist_ok=True)
        with open(self._storage_file, 'w') as f:
            json.dump({
                'jobs': [j.to_dict() for j in self.jobs.values()]
            }, f, indent=2)
    
    def schedule_job(
        self,
        name: str,
        job_type: JobType,
        frequency: ScheduleFrequency,
        job_id: str,
        config: Dict,
        start_time: Optional[datetime] = None
    ) -> ScheduledJob:
        """Schedule a new job"""
        schedule_id = str(uuid.uuid4())[:12]
        
        if start_time is None:
            start_time = datetime.now()
        
        next_run = self._calculate_next_run(start_time, frequency)
        
        scheduled_job = ScheduledJob(
            id=schedule_id,
            name=name,
            job_type=job_type,
            frequency=frequency,
            job_id=job_id,
            config=config,
            next_run=next_run.isoformat()
        )
        
        self.jobs[schedule_id] = scheduled_job
        self._save_jobs()
        
        return scheduled_job
    
    def _calculate_next_run(
        self, 
        from_time: datetime, 
        frequency: ScheduleFrequency
    ) -> datetime:
        """Calculate next run time based on frequency"""
        if frequency == ScheduleFrequency.ONCE:
            return from_time
        elif frequency == ScheduleFrequency.HOURLY:
            return from_time + timedelta(hours=1)
        elif frequency == ScheduleFrequency.DAILY:
            return from_time + timedelta(days=1)
        elif frequency == ScheduleFrequency.WEEKLY:
            return from_time + timedelta(weeks=1)
        elif frequency == ScheduleFrequency.MONTHLY:
            return from_time + timedelta(days=30)
        return from_time
    
    def get_job(self, schedule_id: str) -> Optional[ScheduledJob]:
        """Get a scheduled job"""
        return self.jobs.get(schedule_id)
    
    def list_jobs(self, job_id: str = None) -> List[ScheduledJob]:
        """List scheduled jobs, optionally filtered by conversion job"""
        jobs = list(self.jobs.values())
        if job_id:
            jobs = [j for j in jobs if j.job_id == job_id]
        return sorted(jobs, key=lambda j: j.next_run)
    
    def pause_job(self, schedule_id: str) -> bool:
        """Pause a scheduled job"""
        if schedule_id in self.jobs:
            self.jobs[schedule_id].status = ScheduleStatus.PAUSED
            self._save_jobs()
            return True
        return False
    
    def resume_job(self, schedule_id: str) -> bool:
        """Resume a paused job"""
        if schedule_id in self.jobs:
            self.jobs[schedule_id].status = ScheduleStatus.ACTIVE
            self._save_jobs()
            return True
        return False
    
    def delete_job(self, schedule_id: str) -> bool:
        """Delete a scheduled job"""
        if schedule_id in self.jobs:
            del self.jobs[schedule_id]
            self._save_jobs()
            return True
        return False
    
    def update_after_run(self, schedule_id: str, success: bool, error: str = None):
        """Update job after execution"""
        if schedule_id in self.jobs:
            job = self.jobs[schedule_id]
            job.last_run = datetime.now().isoformat()
            job.run_count += 1
            
            if job.frequency == ScheduleFrequency.ONCE:
                job.status = ScheduleStatus.COMPLETED if success else ScheduleStatus.FAILED
            else:
                job.next_run = self._calculate_next_run(
                    datetime.now(), 
                    job.frequency
                ).isoformat()
            
            self._save_jobs()
    
    def get_due_jobs(self) -> List[ScheduledJob]:
        """Get jobs that are due to run"""
        now = datetime.now()
        due_jobs = []
        
        for job in self.jobs.values():
            if job.status != ScheduleStatus.ACTIVE:
                continue
            
            next_run = datetime.fromisoformat(job.next_run)
            if next_run <= now:
                due_jobs.append(job)
        
        return due_jobs
    
    def get_upcoming_jobs(self, hours: int = 24) -> List[ScheduledJob]:
        """Get jobs scheduled to run in the next N hours"""
        cutoff = datetime.now() + timedelta(hours=hours)
        upcoming = []
        
        for job in self.jobs.values():
            if job.status != ScheduleStatus.ACTIVE:
                continue
            
            next_run = datetime.fromisoformat(job.next_run)
            if next_run <= cutoff:
                upcoming.append(job)
        
        return sorted(upcoming, key=lambda j: j.next_run)


# Global scheduler instance
scheduler = SchedulerService()
