"""
Messaging API Routes
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

from routes.conversion import conversion_jobs
from services.population import get_population_manager
from services.messaging import get_messaging_service, MessageChannel

router = APIRouter()


class CreateTemplateRequest(BaseModel):
    name: str
    subject: str
    body: str
    channel: str = "email"  # email, sms


class PreviewMessageRequest(BaseModel):
    template_id: str
    sample_record: Dict[str, Any]


class SendMessageRequest(BaseModel):
    template_id: str
    population_id: str
    email_column: str = "email"
    phone_column: str = "phone"
    dry_run: bool = True  # Default to dry run for safety


def get_job_info(job_id: str):
    """Get job info and validate it exists"""
    job = conversion_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")
    return job


@router.post("/templates")
async def create_template(request: CreateTemplateRequest, job_id: str = Query(...)):
    """Create a new message template"""
    get_job_info(job_id)
    
    try:
        channel = MessageChannel(request.channel)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid channel. Use: email, sms")
    
    service = get_messaging_service(job_id)
    template = service.create_template(
        name=request.name,
        subject=request.subject,
        body=request.body,
        channel=channel
    )
    
    return {
        "message": "Template created",
        "template": template.to_dict()
    }


@router.get("/templates")
async def list_templates(job_id: str = Query(...)):
    """List all message templates"""
    get_job_info(job_id)
    
    service = get_messaging_service(job_id)
    templates = service.list_templates()
    
    return {
        "count": len(templates),
        "templates": [t.to_dict() for t in templates]
    }


@router.get("/template/{template_id}")
async def get_template(template_id: str, job_id: str = Query(...)):
    """Get a specific template"""
    get_job_info(job_id)
    
    service = get_messaging_service(job_id)
    template = service.get_template(template_id)
    
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    return {"template": template.to_dict()}


@router.delete("/template/{template_id}")
async def delete_template(template_id: str, job_id: str = Query(...)):
    """Delete a template"""
    get_job_info(job_id)
    
    service = get_messaging_service(job_id)
    success = service.delete_template(template_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="Template not found")
    
    return {"message": "Template deleted"}


@router.post("/template/{template_id}/preview")
async def preview_message(
    template_id: str,
    request: PreviewMessageRequest,
    job_id: str = Query(...)
):
    """Preview a message with sample data"""
    get_job_info(job_id)
    
    service = get_messaging_service(job_id)
    preview = service.preview_message(template_id, request.sample_record)
    
    if "error" in preview:
        raise HTTPException(status_code=404, detail=preview["error"])
    
    return preview


@router.post("/messaging/send")
async def send_to_population(request: SendMessageRequest, job_id: str = Query(...)):
    """Send messages to a population"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    if not db_path:
        raise HTTPException(status_code=400, detail="No database for this job")
    
    # Get services
    pop_manager = get_population_manager(job_id, db_path)
    msg_service = get_messaging_service(job_id)
    
    # Validate population exists
    population = pop_manager.get_population(request.population_id)
    if not population:
        raise HTTPException(status_code=404, detail="Population not found")
    
    # Validate template exists
    template = msg_service.get_template(request.template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    try:
        send_job = msg_service.send_to_population(
            population_manager=pop_manager,
            pop_id=request.population_id,
            template_id=request.template_id,
            email_column=request.email_column,
            phone_column=request.phone_column,
            dry_run=request.dry_run
        )
        
        response = {
            "message": "Messages sent" if not request.dry_run else "Dry run completed",
            "send_job": send_job.to_dict(),
            "dry_run": request.dry_run
        }
        
        if request.dry_run:
            response["note"] = "Set dry_run=false to actually send messages"
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/messaging/history")
async def get_send_history(
    job_id: str = Query(...),
    population_id: Optional[str] = Query(None)
):
    """Get message send history"""
    get_job_info(job_id)
    
    service = get_messaging_service(job_id)
    history = service.get_send_history(population_id)
    
    return {
        "count": len(history),
        "history": [h.to_dict() for h in history]
    }


@router.get("/messaging/sample/{pop_id}")
async def get_sample_record(pop_id: str, job_id: str = Query(...)):
    """Get a sample record from population for message preview"""
    job = get_job_info(job_id)
    db_path = job.get("db_path")
    
    if not db_path:
        raise HTTPException(status_code=400, detail="No database for this job")
    
    manager = get_population_manager(job_id, db_path)
    data = manager.get_population_data(pop_id, limit=1)
    
    if "error" in data:
        raise HTTPException(status_code=400, detail=data["error"])
    
    records = data.get("records", [])
    if not records:
        raise HTTPException(status_code=404, detail="No records in population")
    
    return {
        "sample_record": records[0],
        "columns": data.get("columns", [])
    }
