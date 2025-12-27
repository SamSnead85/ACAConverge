"""
AI Lead Routes
API endpoints for AI-powered lead analysis and marketing
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import os

router = APIRouter(prefix="/api/ai", tags=["AI Leads"])


class LeadQueryRequest(BaseModel):
    """Request for lead queries"""
    job_id: str
    question: str


class BestLeadsRequest(BaseModel):
    """Request for best leads"""
    job_id: str
    limit: int = 100
    criteria: str = "ACA enrollment"


class MarketingListRequest(BaseModel):
    """Request for marketing list"""
    job_id: str
    criteria: str = "ACA eligible leads"
    limit: int = 100
    require_email: bool = True


class SendEmailRequest(BaseModel):
    """Request to send marketing email"""
    job_id: str
    lead_ids: List[int]
    template_id: Optional[str] = None
    subject: str = "Your ACA Health Insurance Options"
    enrollment_link: str = ""
    custom_message: Optional[str] = None


def get_lead_service(job_id: str):
    """Get LeadScoringService for a job"""
    from routes.conversion import conversion_jobs
    from services.lead_scoring import LeadScoringService
    
    if job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[job_id]
    
    if not job.get("db_path") or not os.path.exists(job["db_path"]):
        raise HTTPException(status_code=400, detail="Database not available")
    
    return LeadScoringService(
        job["db_path"],
        job.get("table_name", "converted_data")
    )


@router.post("/leads/best")
async def get_best_leads(request: BestLeadsRequest):
    """
    Get AI-recommended best leads for ACA enrollment
    
    Uses Gemini to analyze data and identify high-quality leads.
    """
    service = get_lead_service(request.job_id)
    result = service.get_best_leads(
        limit=request.limit,
        criteria=request.criteria
    )
    return result


@router.post("/leads/ask")
async def ask_about_leads(request: LeadQueryRequest):
    """
    Ask a natural language question about your leads
    
    Examples:
    - "Which leads are best for ACA enrollment?"
    - "Find 50 people I can email about health insurance"
    - "Show me leads in Texas without insurance"
    """
    service = get_lead_service(request.job_id)
    result = service.ask_lead_question(request.question)
    return result


@router.post("/leads/marketing-list")
async def generate_marketing_list(request: MarketingListRequest):
    """
    Generate a targeted marketing list based on criteria
    
    Returns leads that match the criteria and have email addresses.
    """
    service = get_lead_service(request.job_id)
    result = service.generate_marketing_list(
        criteria=request.criteria,
        limit=request.limit,
        require_email=request.require_email
    )
    return result


@router.post("/leads/send-email")
async def send_marketing_email(request: SendEmailRequest):
    """
    Send marketing email to selected leads
    
    Sends ACA enrollment information with personalized content.
    """
    from routes.conversion import conversion_jobs
    from services.messaging import MessagingService
    import sqlite3
    
    if request.job_id not in conversion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = conversion_jobs[request.job_id]
    db_path = job.get("db_path")
    
    if not db_path or not os.path.exists(db_path):
        raise HTTPException(status_code=400, detail="Database not available")
    
    # Get lead service to identify email column
    service = get_lead_service(request.job_id)
    key_cols = service._identify_key_columns()
    email_col = key_cols.get('email')
    name_col = key_cols.get('name')
    
    if not email_col:
        raise HTTPException(status_code=400, detail="No email column identified in data")
    
    # Get lead records
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in request.lead_ids])
        cursor.execute(
            f'SELECT * FROM "converted_data" WHERE _row_id IN ({placeholders})',
            request.lead_ids
        )
        leads = [dict(row) for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    if not leads:
        raise HTTPException(status_code=404, detail="No leads found with given IDs")
    
    # Build email template
    enrollment_link = request.enrollment_link or "https://www.healthcare.gov/"
    
    template = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #6366f1, #8b5cf6); color: white; padding: 30px; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f8fafc; padding: 30px; }}
            .cta {{ display: inline-block; background: #6366f1; color: white; padding: 15px 30px; text-decoration: none; border-radius: 8px; margin: 20px 0; }}
            .footer {{ background: #1e1e2e; color: #888; padding: 20px; font-size: 12px; border-radius: 0 0 10px 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏥 Important Health Insurance Information</h1>
            </div>
            <div class="content">
                <p>Dear {{{{name}}}},</p>
                
                <p>{request.custom_message or "We're reaching out to share important information about affordable health insurance options available to you through the Affordable Care Act (ACA)."}</p>
                
                <h2>Why Consider ACA Coverage?</h2>
                <ul>
                    <li>✅ Comprehensive health coverage</li>
                    <li>✅ Potential premium subsidies based on income</li>
                    <li>✅ No denial for pre-existing conditions</li>
                    <li>✅ Essential health benefits included</li>
                </ul>
                
                <p><strong>Open enrollment is happening now!</strong> Don't miss your chance to get covered.</p>
                
                <p style="text-align: center;">
                    <a href="{enrollment_link}" class="cta">Explore Your Options →</a>
                </p>
                
                <p>Questions? Reply to this email or call us directly.</p>
            </div>
            <div class="footer">
                <p>This message was sent to {{{{email}}}}.</p>
                <p>If you no longer wish to receive these emails, please reply with "UNSUBSCRIBE".</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Send emails
    results = []
    messaging = MessagingService()
    
    for lead in leads:
        email = lead.get(email_col)
        name = lead.get(name_col, "Valued Customer")
        
        if not email:
            results.append({"lead_id": lead.get('_row_id'), "status": "skipped", "reason": "no email"})
            continue
        
        # Personalize template
        personalized = template.replace("{{name}}", str(name)).replace("{{email}}", str(email))
        
        try:
            # Use messaging service to send
            result = await messaging.send_email(
                to_email=email,
                subject=request.subject,
                html_content=personalized
            )
            results.append({
                "lead_id": lead.get('_row_id'),
                "email": email,
                "status": "sent" if result.get("success") else "failed",
                "message": result.get("message", "")
            })
        except Exception as e:
            results.append({
                "lead_id": lead.get('_row_id'),
                "email": email,
                "status": "error",
                "message": str(e)
            })
    
    sent_count = sum(1 for r in results if r["status"] == "sent")
    
    return {
        "success": True,
        "total_leads": len(leads),
        "emails_sent": sent_count,
        "results": results
    }


@router.get("/leads/insights/{job_id}")
async def get_lead_insights(job_id: str):
    """
    Get AI-generated insights about the lead database
    
    Analyzes the data and provides actionable recommendations.
    """
    service = get_lead_service(job_id)
    
    # Get basic stats and insights
    result = service.get_best_leads(limit=1000, criteria="ACA enrollment")
    
    if not result.get("success"):
        return result
    
    insights = result.get("insights", [])
    key_cols = result.get("key_columns", {})
    
    # Add AI-specific insights
    if service.gemini_model:
        try:
            columns = [col['name'] for col in service.schema]
            prompt = f"""Analyze this database for ACA enrollment sales opportunities.

Columns available: {', '.join(columns)}
Key columns identified: {key_cols}
Total leads analyzed: {result.get('count', 0)}

Provide 3-5 actionable insights for an ACA enrollment sales team.
Format as JSON array with objects containing: title, description, priority (high/medium/low), action
"""
            response = service.gemini_model.generate_content(prompt)
            text = response.text.strip()
            
            if '```json' in text:
                text = text.split('```json')[1].split('```')[0]
            elif '```' in text:
                text = text.split('```')[1].split('```')[0]
            
            import json
            ai_insights = json.loads(text)
            insights.extend([{**i, "type": "ai"} for i in ai_insights])
            
        except Exception as e:
            insights.append({
                "type": "error",
                "title": "AI Analysis",
                "description": f"Could not generate AI insights: {str(e)}"
            })
    
    return {
        "success": True,
        "insights": insights,
        "key_columns": key_cols,
        "total_records": result.get("count", 0)
    }
