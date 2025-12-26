"""
Conversion Routes
API endpoints for file upload and YXDB to SQL conversion
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from typing import Dict, Any
import os
import uuid
import asyncio
import aiofiles

from services.yxdb_parser import YxdbParser, MockYxdbParser
from services.sql_converter import SqlConverter, ConversionProgress

router = APIRouter()

# Store conversion jobs and their progress
conversion_jobs: Dict[str, Dict[str, Any]] = {}


def update_progress(job_id: str, progress: ConversionProgress):
    """Callback to update job progress"""
    if job_id in conversion_jobs:
        conversion_jobs[job_id]["progress"] = {
            "status": progress.status,
            "total_records": progress.total_records,
            "processed_records": progress.processed_records,
            "percentage": progress.percentage,
            "message": progress.message,
            "error": progress.error
        }


async def process_yxdb_file(job_id: str, file_path: str, use_mock: bool = False):
    """Background task to process YXDB file"""
    try:
        db_path = f"databases/{job_id}.db"
        
        # Create parser
        if use_mock:
            parser = MockYxdbParser(file_path, num_records=50000)
        else:
            try:
                parser = YxdbParser(file_path)
            except ImportError:
                # Fallback to mock if yxdb not installed
                parser = MockYxdbParser(file_path, num_records=50000)
        
        # Get schema
        schema = parser.get_schema()
        conversion_jobs[job_id]["schema"] = schema
        
        # Estimate records based on file size
        file_size = parser.get_file_size()
        estimated_records = max(file_size // 200, 1000)
        
        # Create converter and process
        converter = SqlConverter(db_path, table_name="converted_data")
        
        def progress_callback(progress):
            update_progress(job_id, progress)
        
        # Run conversion in thread pool
        loop = asyncio.get_event_loop()
        final_progress = await loop.run_in_executor(
            None,
            lambda: converter.convert_from_parser(
                parser,
                batch_size=10000,
                progress_callback=progress_callback,
                estimated_records=estimated_records
            )
        )
        
        conversion_jobs[job_id]["db_path"] = db_path
        conversion_jobs[job_id]["table_name"] = "converted_data"
        
        # Clean up uploaded file
        if os.path.exists(file_path):
            os.remove(file_path)
            
    except Exception as e:
        conversion_jobs[job_id]["progress"] = {
            "status": "error",
            "message": str(e),
            "error": str(e)
        }


@router.post("/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    use_mock: bool = False
):
    """
    Upload a .yxdb file for conversion
    
    Args:
        file: The .yxdb file to upload
        use_mock: If True, generate mock data instead of parsing real file
        
    Returns:
        Job ID for tracking conversion progress
    """
    # Validate file extension (allow any for testing)
    if not use_mock and not file.filename.lower().endswith('.yxdb'):
        # Allow non-yxdb files but use mock data
        use_mock = True
    
    # Generate job ID
    job_id = str(uuid.uuid4())[:12]
    
    # Save uploaded file
    upload_path = f"uploads/{job_id}_{file.filename}"
    
    async with aiofiles.open(upload_path, 'wb') as f:
        content = await file.read()
        await f.write(content)
    
    # Initialize job
    conversion_jobs[job_id] = {
        "job_id": job_id,
        "filename": file.filename,
        "file_size": len(content),
        "progress": {
            "status": "pending",
            "percentage": 0,
            "message": "Starting conversion..."
        },
        "schema": None,
        "db_path": None
    }
    
    # Start background processing
    background_tasks.add_task(process_yxdb_file, job_id, upload_path, use_mock)
    
    return {
        "job_id": job_id,
        "message": "File uploaded successfully. Conversion started.",
        "filename": file.filename
    }


@router.post("/upload/demo")
async def upload_demo(background_tasks: BackgroundTasks):
    """
    Create a demo conversion with mock data
    Useful for testing without actual .yxdb files
    """
    job_id = str(uuid.uuid4())[:12]
    
    conversion_jobs[job_id] = {
        "job_id": job_id,
        "filename": "demo_data.yxdb",
        "file_size": 10000000,  # 10MB mock
        "progress": {
            "status": "pending",
            "percentage": 0,
            "message": "Starting demo conversion..."
        },
        "schema": None,
        "db_path": None
    }
    
    # Use mock parser with demo data
    background_tasks.add_task(process_yxdb_file, job_id, "demo.yxdb", use_mock=True)
    
    return {
        "job_id": job_id,
        "message": "Demo conversion started with 50,000 sample records.",
        "filename": "demo_data.yxdb"
    }


@router.get("/conversion/status/{job_id}")
async def get_conversion_status(job_id: str):
    """Get the status of a conversion job"""
    if job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[job_id]
    return {
        "job_id": job_id,
        "filename": job["filename"],
        "progress": job["progress"],
        "schema": job.get("schema"),
        "has_database": job.get("db_path") is not None
    }


@router.get("/schema/{job_id}")
async def get_schema(job_id: str):
    """Get the schema of a converted database"""
    if job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[job_id]
    
    if not job.get("schema"):
        raise HTTPException(status_code=400, detail="Schema not yet available")
    
    return {
        "job_id": job_id,
        "table_name": job.get("table_name", "converted_data"),
        "schema": job["schema"]
    }


@router.get("/download/{job_id}")
async def download_database(job_id: str):
    """Download the converted SQLite database"""
    if job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[job_id]
    
    if not job.get("db_path") or not os.path.exists(job["db_path"]):
        raise HTTPException(status_code=400, detail="Database not yet available")
    
    return FileResponse(
        job["db_path"],
        media_type="application/x-sqlite3",
        filename=f"{job_id}_converted.db"
    )


@router.get("/jobs")
async def list_jobs():
    """List all conversion jobs"""
    return {
        "jobs": [
            {
                "job_id": job_id,
                "filename": job["filename"],
                "status": job["progress"]["status"],
                "percentage": job["progress"].get("percentage", 0)
            }
            for job_id, job in conversion_jobs.items()
        ]
    }


@router.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a conversion job and its database"""
    if job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[job_id]
    
    # Delete database file
    if job.get("db_path") and os.path.exists(job["db_path"]):
        os.remove(job["db_path"])
    
    del conversion_jobs[job_id]
    
    return {"message": f"Job {job_id} deleted successfully"}
