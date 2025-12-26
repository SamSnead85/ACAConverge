"""
Population Management API Routes
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

from routes.conversion import conversion_jobs
from services.population import get_population_manager, CombineOperation

router = APIRouter()


class CreatePopulationRequest(BaseModel):
    name: str
    query: str
    natural_language: str = ""
    description: str = ""
    tags: List[str] = []


class CombinePopulationsRequest(BaseModel):
    population_ids: List[str]
    operation: str  # union, intersect, exclude
    new_name: str


def get_job_info(job_id: str):
    """Get job info and validate it exists"""
    job = conversion_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")
    return job


@router.post("/populations/{job_id}")
async def create_population(job_id: str, request: CreatePopulationRequest):
    """Create a new population from a query"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    if not db_path:
        raise HTTPException(status_code=400, detail="No database for this job")
    
    manager = get_population_manager(job_id, db_path)
    population = manager.create_population(
        name=request.name,
        query=request.query,
        natural_language=request.natural_language,
        description=request.description,
        tags=request.tags
    )
    
    return {
        "message": "Population created",
        "population": population.to_dict()
    }


@router.get("/populations/{job_id}")
async def list_populations(job_id: str):
    """List all populations for a job"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    if not db_path:
        return {"populations": []}
    
    manager = get_population_manager(job_id, db_path)
    populations = manager.list_populations()
    
    return {
        "job_id": job_id,
        "count": len(populations),
        "populations": [p.to_dict() for p in populations]
    }


@router.get("/population/{pop_id}")
async def get_population(pop_id: str, job_id: str = Query(...)):
    """Get a specific population"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    manager = get_population_manager(job_id, db_path)
    population = manager.get_population(pop_id)
    
    if not population:
        raise HTTPException(status_code=404, detail="Population not found")
    
    return {"population": population.to_dict()}


@router.delete("/population/{pop_id}")
async def delete_population(pop_id: str, job_id: str = Query(...)):
    """Delete a population"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    manager = get_population_manager(job_id, db_path)
    success = manager.delete_population(pop_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="Population not found")
    
    return {"message": "Population deleted"}


@router.get("/population/{pop_id}/data")
async def get_population_data(
    pop_id: str,
    job_id: str = Query(...),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    """Get records for a population"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    manager = get_population_manager(job_id, db_path)
    result = manager.get_population_data(pop_id, limit=limit, offset=offset)
    
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    return result


@router.post("/population/{pop_id}/refresh")
async def refresh_population(pop_id: str, job_id: str = Query(...)):
    """Refresh population count"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    manager = get_population_manager(job_id, db_path)
    population = manager.refresh_population(pop_id)
    
    if not population:
        raise HTTPException(status_code=404, detail="Population not found")
    
    return {
        "message": "Population refreshed",
        "population": population.to_dict()
    }


@router.get("/population/{pop_id}/stats")
async def get_population_stats(pop_id: str, job_id: str = Query(...)):
    """Get statistical summary of a population"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    manager = get_population_manager(job_id, db_path)
    stats = manager.get_population_stats(pop_id)
    
    if "error" in stats:
        raise HTTPException(status_code=400, detail=stats["error"])
    
    return stats


@router.post("/populations/combine")
async def combine_populations(request: CombinePopulationsRequest, job_id: str = Query(...)):
    """Combine multiple populations"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    try:
        operation = CombineOperation(request.operation)
    except ValueError:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid operation. Use: union, intersect, exclude"
        )
    
    manager = get_population_manager(job_id, db_path)
    population = manager.combine_populations(
        pop_ids=request.population_ids,
        operation=operation,
        new_name=request.new_name
    )
    
    if not population:
        raise HTTPException(status_code=400, detail="Failed to combine populations")
    
    return {
        "message": f"Populations combined using {request.operation}",
        "population": population.to_dict()
    }
