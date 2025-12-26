"""
File processing utilities and queue management
"""

import os
import asyncio
import uuid
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import json


class JobStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    ERROR = "error"
    CANCELLED = "cancelled"


@dataclass
class ConversionJob:
    """Represents a file conversion job"""
    job_id: str
    filename: str
    file_size: int
    status: JobStatus = JobStatus.PENDING
    progress: float = 0.0
    message: str = ""
    records_processed: int = 0
    total_records: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    schema: Optional[List[Dict]] = None
    db_path: Optional[str] = None
    table_name: str = "converted_data"
    
    def to_dict(self) -> Dict:
        return {
            "job_id": self.job_id,
            "filename": self.filename,
            "file_size": self.file_size,
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "records_processed": self.records_processed,
            "total_records": self.total_records,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "schema": self.schema,
            "has_database": self.db_path is not None
        }


class JobQueue:
    """
    Simple in-memory job queue
    For production, use Redis or a proper message queue
    """
    
    def __init__(self, max_concurrent: int = 2):
        self.max_concurrent = max_concurrent
        self.jobs: Dict[str, ConversionJob] = {}
        self.queue: List[str] = []
        self.processing: List[str] = []
        self._lock = asyncio.Lock()
    
    async def add_job(self, job: ConversionJob) -> str:
        """Add a job to the queue"""
        async with self._lock:
            self.jobs[job.job_id] = job
            self.queue.append(job.job_id)
            job.status = JobStatus.QUEUED
            job.message = f"Queued (position {len(self.queue)})"
        return job.job_id
    
    async def get_job(self, job_id: str) -> Optional[ConversionJob]:
        """Get a job by ID"""
        return self.jobs.get(job_id)
    
    async def update_job(self, job_id: str, **updates):
        """Update job properties"""
        if job_id in self.jobs:
            job = self.jobs[job_id]
            for key, value in updates.items():
                if hasattr(job, key):
                    setattr(job, key, value)
    
    async def get_next_job(self) -> Optional[ConversionJob]:
        """Get next job to process"""
        async with self._lock:
            if len(self.processing) >= self.max_concurrent:
                return None
            
            if not self.queue:
                return None
            
            job_id = self.queue.pop(0)
            self.processing.append(job_id)
            job = self.jobs[job_id]
            job.status = JobStatus.PROCESSING
            job.started_at = datetime.now().isoformat()
            job.message = "Processing..."
            return job
    
    async def complete_job(self, job_id: str, success: bool = True, error: str = None):
        """Mark a job as completed"""
        async with self._lock:
            if job_id in self.processing:
                self.processing.remove(job_id)
            
            if job_id in self.jobs:
                job = self.jobs[job_id]
                job.completed_at = datetime.now().isoformat()
                
                if success:
                    job.status = JobStatus.COMPLETED
                    job.progress = 100.0
                    job.message = f"Completed: {job.records_processed:,} records processed"
                else:
                    job.status = JobStatus.ERROR
                    job.error = error
                    job.message = f"Error: {error}"
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a queued job"""
        async with self._lock:
            if job_id in self.queue:
                self.queue.remove(job_id)
                if job_id in self.jobs:
                    self.jobs[job_id].status = JobStatus.CANCELLED
                    self.jobs[job_id].message = "Cancelled by user"
                return True
            return False
    
    def get_queue_status(self) -> Dict:
        """Get queue status"""
        return {
            "queued": len(self.queue),
            "processing": len(self.processing),
            "max_concurrent": self.max_concurrent,
            "total_jobs": len(self.jobs)
        }
    
    def get_all_jobs(self, limit: int = 50) -> List[Dict]:
        """Get all jobs"""
        jobs = list(self.jobs.values())
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return [j.to_dict() for j in jobs[:limit]]


class FileCleanupService:
    """Service for cleaning up old files and databases"""
    
    def __init__(
        self,
        upload_dir: str = "uploads",
        database_dir: str = "databases",
        max_age_hours: int = 24
    ):
        self.upload_dir = upload_dir
        self.database_dir = database_dir
        self.max_age_hours = max_age_hours
    
    def cleanup_old_files(self) -> Dict:
        """Remove files older than max_age_hours"""
        removed = {"uploads": 0, "databases": 0, "bytes_freed": 0}
        cutoff = datetime.now() - timedelta(hours=self.max_age_hours)
        
        # Cleanup uploads
        if os.path.exists(self.upload_dir):
            for filename in os.listdir(self.upload_dir):
                filepath = os.path.join(self.upload_dir, filename)
                if os.path.isfile(filepath):
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                    if mtime < cutoff:
                        size = os.path.getsize(filepath)
                        os.remove(filepath)
                        removed["uploads"] += 1
                        removed["bytes_freed"] += size
        
        # Cleanup databases
        if os.path.exists(self.database_dir):
            for filename in os.listdir(self.database_dir):
                filepath = os.path.join(self.database_dir, filename)
                if os.path.isfile(filepath):
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                    if mtime < cutoff:
                        size = os.path.getsize(filepath)
                        os.remove(filepath)
                        removed["databases"] += 1
                        removed["bytes_freed"] += size
        
        return removed
    
    def get_storage_stats(self) -> Dict:
        """Get storage usage statistics"""
        def dir_size(path):
            total = 0
            count = 0
            if os.path.exists(path):
                for entry in os.scandir(path):
                    if entry.is_file():
                        total += entry.stat().st_size
                        count += 1
            return {"bytes": total, "files": count}
        
        return {
            "uploads": dir_size(self.upload_dir),
            "databases": dir_size(self.database_dir),
            "max_age_hours": self.max_age_hours
        }


class DataPreview:
    """Generate data previews before full conversion"""
    
    @staticmethod
    def sample_file(parser, sample_size: int = 100) -> Dict:
        """Get a sample of data from the file"""
        schema = parser.get_schema()
        sample_data = []
        
        for batch in parser.stream_records(batch_size=sample_size):
            sample_data.extend(batch[:sample_size - len(sample_data)])
            if len(sample_data) >= sample_size:
                break
        
        # Generate column statistics
        column_stats = []
        for col in schema:
            values = [row.get(col["name"]) for row in sample_data]
            non_null = [v for v in values if v is not None]
            
            stats = {
                "name": col["name"],
                "type": col["sql_type"],
                "null_count": len(values) - len(non_null),
                "unique_count": len(set(str(v) for v in non_null)),
                "sample_values": [str(v) for v in non_null[:5]]
            }
            
            # Numeric stats
            numeric_values = [v for v in non_null if isinstance(v, (int, float))]
            if numeric_values:
                stats["min"] = min(numeric_values)
                stats["max"] = max(numeric_values)
                stats["avg"] = sum(numeric_values) / len(numeric_values)
            
            column_stats.append(stats)
        
        return {
            "sample_size": len(sample_data),
            "schema": schema,
            "column_stats": column_stats,
            "sample_data": sample_data[:10]
        }


# Global instances
job_queue = JobQueue(max_concurrent=2)
cleanup_service = FileCleanupService()
