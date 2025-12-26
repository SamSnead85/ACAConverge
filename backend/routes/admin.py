"""
Admin routes for monitoring, metrics, and management
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Optional
import os
import platform
import psutil
from datetime import datetime

from services.middleware import metrics
from services.cache import cache, query_cache
from services.file_processing import job_queue, cleanup_service

router = APIRouter()


@router.get("/admin/health")
async def health_check():
    """Detailed health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }


@router.get("/admin/metrics")
async def get_metrics():
    """Get API metrics"""
    return metrics.get_metrics()


@router.get("/admin/cache/stats")
async def get_cache_stats():
    """Get cache statistics"""
    return {
        "general_cache": cache.get_stats(),
        "query_cache": query_cache.get_stats()
    }


@router.post("/admin/cache/clear")
async def clear_cache():
    """Clear all caches"""
    cache.clear()
    return {"message": "Cache cleared successfully"}


@router.get("/admin/queue/status")
async def get_queue_status():
    """Get job queue status"""
    return job_queue.get_queue_status()


@router.get("/admin/jobs")
async def list_all_jobs(limit: int = 50):
    """List all conversion jobs"""
    return {"jobs": job_queue.get_all_jobs(limit)}


@router.get("/admin/storage")
async def get_storage_stats():
    """Get storage usage statistics"""
    return cleanup_service.get_storage_stats()


@router.post("/admin/cleanup")
async def trigger_cleanup():
    """Manually trigger file cleanup"""
    result = cleanup_service.cleanup_old_files()
    return {
        "message": "Cleanup completed",
        "result": result
    }


@router.get("/admin/system")
async def get_system_info():
    """Get system information"""
    try:
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": platform.python_version(),
            "cpu_count": psutil.cpu_count(),
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory": {
                "total_gb": round(memory.total / (1024**3), 2),
                "available_gb": round(memory.available / (1024**3), 2),
                "percent_used": memory.percent
            },
            "disk": {
                "total_gb": round(disk.total / (1024**3), 2),
                "free_gb": round(disk.free / (1024**3), 2),
                "percent_used": round((disk.used / disk.total) * 100, 1)
            }
        }
    except Exception as e:
        return {
            "error": str(e),
            "platform": platform.system(),
            "python_version": platform.python_version()
        }


@router.get("/admin/config")
async def get_config():
    """Get current configuration (non-sensitive)"""
    return {
        "gemini_configured": bool(os.getenv("GEMINI_API_KEY")),
        "frontend_url": os.getenv("FRONTEND_URL", "not set"),
        "upload_dir": "uploads",
        "database_dir": "databases",
        "max_file_age_hours": 24,
        "max_concurrent_jobs": 2,
        "cache_max_size": 500,
        "cache_ttl_seconds": 300
    }
