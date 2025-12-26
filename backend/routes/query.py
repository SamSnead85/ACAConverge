"""
Query Routes
API endpoints for NLP queries and result export
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import os

from services.nlp_query import NlpQueryService

router = APIRouter()

# Cache for query services per database
query_services: Dict[str, NlpQueryService] = {}


class QueryRequest(BaseModel):
    """Request model for NLP query"""
    job_id: str
    question: str
    max_rows: int = 1000


class SqlQueryRequest(BaseModel):
    """Request model for direct SQL query"""
    job_id: str
    sql: str
    max_rows: int = 1000


def get_query_service(job_id: str) -> NlpQueryService:
    """Get or create query service for a job"""
    from routes.conversion import conversion_jobs
    
    if job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[job_id]
    
    if not job.get("db_path") or not os.path.exists(job["db_path"]):
        raise HTTPException(status_code=400, detail="Database not yet available")
    
    # Create or get cached service
    if job_id not in query_services:
        query_services[job_id] = NlpQueryService(
            job["db_path"],
            job.get("table_name", "converted_data")
        )
    
    return query_services[job_id]


@router.post("/query")
async def submit_query(request: QueryRequest):
    """
    Submit a natural language query
    
    The query is converted to SQL using Gemini AI and executed
    """
    service = get_query_service(request.job_id)
    
    result = service.process_natural_language_query(
        request.question,
        max_rows=request.max_rows
    )
    
    return {
        "query_id": result.query_id,
        "natural_language": result.natural_language,
        "sql_query": result.sql_query,
        "columns": result.columns,
        "results": result.results,
        "row_count": result.row_count,
        "execution_time_ms": result.execution_time_ms,
        "error": result.error
    }


@router.post("/query/sql")
async def submit_sql_query(request: SqlQueryRequest):
    """
    Submit a direct SQL query
    
    Only SELECT queries are allowed
    """
    service = get_query_service(request.job_id)
    
    try:
        results, columns, count = service.execute_query(
            request.sql,
            max_rows=request.max_rows
        )
        
        return {
            "sql_query": request.sql,
            "columns": columns,
            "results": results,
            "row_count": count,
            "error": None
        }
        
    except Exception as e:
        return {
            "sql_query": request.sql,
            "columns": [],
            "results": [],
            "row_count": 0,
            "error": str(e)
        }


@router.get("/query/history/{job_id}")
async def get_query_history(job_id: str, limit: int = 20):
    """Get query history for a job"""
    service = get_query_service(job_id)
    return {"history": service.get_history(limit)}


@router.get("/query/export/{job_id}/{query_id}")
async def export_query_results(
    job_id: str,
    query_id: str,
    format: str = "json"
):
    """
    Export query results in specified format
    
    Args:
        job_id: The conversion job ID
        query_id: The query ID to export
        format: 'json' or 'csv'
    """
    service = get_query_service(job_id)
    
    # Find the query in history
    for result in service.query_history:
        if result.query_id == query_id:
            export_data = service.export_results(result, format)
            
            if format == "csv":
                return Response(
                    content=export_data,
                    media_type="text/csv",
                    headers={
                        "Content-Disposition": f"attachment; filename=query_{query_id}.csv"
                    }
                )
            else:
                return Response(
                    content=export_data,
                    media_type="application/json",
                    headers={
                        "Content-Disposition": f"attachment; filename=query_{query_id}.json"
                    }
                )
    
    raise HTTPException(status_code=404, detail="Query not found in history")


@router.get("/query/schema/{job_id}")
async def get_database_schema(job_id: str):
    """Get database schema for query context"""
    service = get_query_service(job_id)
    return {"schema_context": service.schema_context}


@router.post("/query/preview")
async def preview_sql(request: QueryRequest):
    """
    Preview the SQL that would be generated from a natural language query
    without executing it
    """
    service = get_query_service(request.job_id)
    
    # Generate SQL without executing
    sql = service._generate_sql_with_gemini(request.question)
    
    return {
        "natural_language": request.question,
        "sql_query": sql
    }


@router.get("/query/suggestions/{job_id}")
async def get_query_suggestions(job_id: str):
    """
    Get AI-powered query suggestions based on the data schema
    """
    from routes.conversion import conversion_jobs
    from services.smart_suggestions import SmartQuerySuggestions
    
    if job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[job_id]
    schema = job.get("schema", [])
    
    if not schema:
        return {"suggestions": []}
    
    suggestions_service = SmartQuerySuggestions(schema)
    suggestions = suggestions_service.generate_suggestions()
    
    return {"suggestions": suggestions}


@router.post("/query/alternatives")
async def get_alternative_queries(request: QueryRequest):
    """
    Get alternative query suggestions when a query fails or returns no results
    """
    from routes.conversion import conversion_jobs
    from services.smart_suggestions import SmartQuerySuggestions
    
    if request.job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[request.job_id]
    schema = job.get("schema", [])
    
    suggestions_service = SmartQuerySuggestions(schema)
    alternatives = suggestions_service.get_alternative_queries(request.question)
    
    return {"alternatives": alternatives}


@router.post("/query/refine")
async def refine_query(request: QueryRequest):
    """
    Use AI to refine and improve a query before execution
    """
    from routes.conversion import conversion_jobs
    from services.smart_suggestions import SmartQuerySuggestions
    
    if request.job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[request.job_id]
    schema = job.get("schema", [])
    
    suggestions_service = SmartQuerySuggestions(schema)
    refined = suggestions_service.refine_query(request.question)
    
    return refined
