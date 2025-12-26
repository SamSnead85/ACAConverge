"""
Conversion Routes
API endpoints for file upload and multi-format to SQL conversion
Supports: YXDB, CSV, Excel (.xlsx/.xls), JSON
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse
from typing import Dict, Any, List, Optional
import os
import uuid
import asyncio
import aiofiles

from services.yxdb_parser import YxdbParser, MockYxdbParser
from services.sql_converter import SqlConverter, ConversionProgress
from services.file_parser import get_parser, SUPPORTED_EXTENSIONS, MAX_FILE_SIZES

router = APIRouter()

# Store conversion jobs and their progress
conversion_jobs: Dict[str, Dict[str, Any]] = {}

# Increase upload limits
MAX_UPLOAD_SIZE = 50 * 1024 * 1024 * 1024  # 50 GB


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


async def process_file(job_id: str, file_path: str, use_mock: bool = False):
    """Background task to process any supported file format"""
    try:
        db_path = f"databases/{job_id}.db"
        
        # Get file extension
        ext = os.path.splitext(file_path)[1].lower()
        
        if use_mock:
            # Use mock parser
            parser = MockYxdbParser(file_path, num_records=50000)
        else:
            try:
                # Use appropriate parser based on file type
                parser = get_parser(file_path)
            except ValueError as e:
                # Unsupported format - use mock
                parser = MockYxdbParser(file_path, num_records=50000)
            except ImportError as e:
                # Missing dependency
                conversion_jobs[job_id]["progress"] = {
                    "status": "error",
                    "message": f"Missing dependency: {str(e)}",
                    "error": str(e)
                }
                return
        
        # Get schema
        schema = parser.get_schema()
        
        # Convert schema to dict format
        if hasattr(schema[0], 'name'):
            # ColumnInfo objects
            schema_list = [
                {"name": col.name, "type": col.data_type, "sql_type": col.sql_type}
                for col in schema
            ]
        else:
            # Already dict format
            schema_list = schema
        
        conversion_jobs[job_id]["schema"] = schema_list
        
        # Get estimated records
        try:
            estimated_records = parser.get_row_count()
        except:
            # Estimate based on file size
            file_size = os.path.getsize(file_path)
            estimated_records = max(file_size // 200, 1000)
        
        # Create converter and process
        converter = SqlConverter(db_path, table_name="converted_data")
        
        def progress_callback(progress):
            update_progress(job_id, progress)
        
        # Run conversion in thread pool
        loop = asyncio.get_event_loop()
        
        # Adapt parser for SqlConverter
        async def run_conversion():
            return await loop.run_in_executor(
                None,
                lambda: converter.convert_from_parser(
                    parser,
                    batch_size=10000,
                    progress_callback=progress_callback,
                    estimated_records=estimated_records
                )
            )
        
        await run_conversion()
        
        conversion_jobs[job_id]["db_path"] = db_path
        conversion_jobs[job_id]["table_name"] = "converted_data"
        conversion_jobs[job_id]["status"] = "completed"
        
        # Clean up uploaded file to save space
        if os.path.exists(file_path):
            os.remove(file_path)
            
    except Exception as e:
        import traceback
        conversion_jobs[job_id]["progress"] = {
            "status": "error",
            "message": str(e),
            "error": traceback.format_exc()
        }


@router.post("/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    use_mock: bool = False
):
    """
    Upload a file for conversion to SQL database
    
    Supported formats:
    - .yxdb (Alteryx Database)
    - .csv (Comma-Separated Values)
    - .xlsx / .xls (Excel)
    - .json (JSON array or newline-delimited)
    
    Args:
        file: The file to upload
        use_mock: If True, generate mock data instead of parsing
        
    Returns:
        Job ID for tracking conversion progress
    """
    # Get file extension
    ext = os.path.splitext(file.filename)[1].lower()
    
    # Validate file extension
    if ext not in SUPPORTED_EXTENSIONS and not use_mock:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format: {ext}. Supported: {', '.join(SUPPORTED_EXTENSIONS.keys())}"
        )
    
    # Generate job ID
    job_id = str(uuid.uuid4())[:12]
    
    # Save uploaded file with streaming for large files
    upload_path = f"uploads/{job_id}_{file.filename}"
    
    # Ensure uploads directory exists
    os.makedirs("uploads", exist_ok=True)
    
    # Stream file to disk (handles large files)
    total_size = 0
    chunk_size = 10 * 1024 * 1024  # 10MB chunks
    
    async with aiofiles.open(upload_path, 'wb') as f:
        while True:
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            await f.write(chunk)
            total_size += len(chunk)
            
            # Check max size
            if total_size > MAX_UPLOAD_SIZE:
                os.remove(upload_path)
                raise HTTPException(
                    status_code=413,
                    detail=f"File too large. Maximum size: {MAX_UPLOAD_SIZE // (1024*1024*1024)} GB"
                )
    
    # Initialize job
    conversion_jobs[job_id] = {
        "job_id": job_id,
        "filename": file.filename,
        "file_type": ext,
        "file_size": total_size,
        "progress": {
            "status": "pending",
            "percentage": 0,
            "message": f"Starting conversion of {ext} file..."
        },
        "schema": None,
        "db_path": None
    }
    
    # Start background processing
    background_tasks.add_task(process_file, job_id, upload_path, use_mock)
    
    return {
        "job_id": job_id,
        "message": f"File uploaded successfully ({total_size / (1024*1024):.1f} MB). Conversion started.",
        "filename": file.filename,
        "file_type": SUPPORTED_EXTENSIONS.get(ext, "Unknown")
    }


@router.get("/supported-formats")
async def get_supported_formats():
    """Get list of supported file formats"""
    return {
        "formats": [
            {"extension": ext, "name": name}
            for ext, name in SUPPORTED_EXTENSIONS.items()
        ],
        "max_file_size_gb": MAX_UPLOAD_SIZE // (1024*1024*1024)
    }


@router.post("/upload/demo")
async def upload_demo(
    background_tasks: BackgroundTasks,
    record_count: int = Query(50000, ge=1000, le=1000000)
):
    """
    Create a demo conversion with mock data
    Useful for testing without actual files
    
    Args:
        record_count: Number of mock records to generate (1,000 - 1,000,000)
    """
    job_id = str(uuid.uuid4())[:12]
    
    conversion_jobs[job_id] = {
        "job_id": job_id,
        "filename": "demo_data.yxdb",
        "file_type": ".yxdb",
        "file_size": record_count * 200,
        "progress": {
            "status": "pending",
            "percentage": 0,
            "message": f"Generating {record_count:,} demo records..."
        },
        "schema": None,
        "db_path": None
    }
    
    # Use mock parser with specified record count
    async def process_demo():
        try:
            parser = MockYxdbParser("demo.yxdb", num_records=record_count)
            db_path = f"databases/{job_id}.db"
            
            schema = parser.get_schema()
            conversion_jobs[job_id]["schema"] = schema
            
            converter = SqlConverter(db_path, table_name="converted_data")
            
            def progress_callback(progress):
                update_progress(job_id, progress)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: converter.convert_from_parser(
                    parser,
                    batch_size=10000,
                    progress_callback=progress_callback,
                    estimated_records=record_count
                )
            )
            
            conversion_jobs[job_id]["db_path"] = db_path
            conversion_jobs[job_id]["table_name"] = "converted_data"
            
        except Exception as e:
            conversion_jobs[job_id]["progress"] = {
                "status": "error",
                "message": str(e),
                "error": str(e)
            }
    
    background_tasks.add_task(process_demo)
    
    return {
        "job_id": job_id,
        "message": f"Demo conversion started with {record_count:,} sample records.",
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
        "file_type": job.get("file_type", "unknown"),
        "file_size": job.get("file_size", 0),
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
                "file_type": job.get("file_type", "unknown"),
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
