"""
Reporting API Routes
"""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import List, Optional

from routes.conversion import conversion_jobs
from services.population import get_population_manager
from services.reporting import ReportGenerator

router = APIRouter()


class DetailedReportRequest(BaseModel):
    columns: Optional[List[str]] = None
    limit: int = 10000
    name: Optional[str] = None


class ComparisonReportRequest(BaseModel):
    population_ids: List[str]
    name: Optional[str] = None


def get_job_and_manager(job_id: str):
    """Get job info and population manager"""
    job = conversion_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")
    
    db_path = job.get("db_path")
    if not db_path:
        raise HTTPException(status_code=400, detail="No database for this job")
    
    manager = get_population_manager(job_id, db_path)
    return job, manager


@router.post("/report/summary/{pop_id}")
async def generate_summary_report(
    pop_id: str,
    job_id: str = Query(...),
    name: Optional[str] = None
):
    """Generate a summary statistics report"""
    job, manager = get_job_and_manager(job_id)
    generator = ReportGenerator(manager)
    
    try:
        report = generator.generate_summary_report(pop_id, name)
        return {"report": report.to_dict()}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/report/detailed/{pop_id}")
async def generate_detailed_report(
    pop_id: str,
    job_id: str = Query(...),
    request: DetailedReportRequest = DetailedReportRequest()
):
    """Generate a detailed data report"""
    job, manager = get_job_and_manager(job_id)
    generator = ReportGenerator(manager)
    
    try:
        report = generator.generate_detailed_report(
            pop_id,
            columns=request.columns,
            limit=request.limit,
            name=request.name
        )
        return {"report": report.to_dict()}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/report/comparison")
async def generate_comparison_report(
    request: ComparisonReportRequest,
    job_id: str = Query(...)
):
    """Generate a comparison report for multiple populations"""
    job, manager = get_job_and_manager(job_id)
    generator = ReportGenerator(manager)
    
    report = generator.generate_comparison_report(
        request.population_ids,
        name=request.name
    )
    
    return {"report": report.to_dict()}


@router.get("/report/{pop_id}/html")
async def get_report_html(
    pop_id: str,
    job_id: str = Query(...),
    type: str = Query("summary")  # summary, detailed, comparison
):
    """Get HTML preview of a report"""
    job, manager = get_job_and_manager(job_id)
    generator = ReportGenerator(manager)
    
    try:
        if type == "summary":
            report = generator.generate_summary_report(pop_id)
        elif type == "detailed":
            report = generator.generate_detailed_report(pop_id, limit=100)
        else:
            raise HTTPException(status_code=400, detail="Invalid report type")
        
        html = generator.generate_html_report(report)
        return HTMLResponse(content=html)
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/report/{pop_id}/download")
async def download_report(
    pop_id: str,
    job_id: str = Query(...),
    format: str = Query("csv"),  # csv, json
    type: str = Query("detailed")  # summary, detailed
):
    """Download a report in specified format"""
    job, manager = get_job_and_manager(job_id)
    generator = ReportGenerator(manager)
    
    try:
        if type == "summary":
            report = generator.generate_summary_report(pop_id)
        else:
            report = generator.generate_detailed_report(pop_id)
        
        if format == "csv":
            filepath = generator.export_to_csv(report)
            media_type = "text/csv"
        else:
            filepath = generator.export_to_json(report)
            media_type = "application/json"
        
        population = manager.get_population(pop_id)
        filename = f"{population.name.replace(' ', '_')}_{type}.{format}"
        
        return FileResponse(
            path=filepath,
            media_type=media_type,
            filename=filename
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
