"""
Member Search Routes for ACA DataHub
CRM-style member search, advanced filtering, and AI-powered analysis
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, date
import sqlite3
import os
import re

router = APIRouter(prefix="/members", tags=["Member CRM"])

# Database path
DATABASES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "databases")


# =========================================================================
# Models
# =========================================================================

class QuickSearchRequest(BaseModel):
    job_id: str
    query: str
    limit: int = 50


class AdvancedSearchRequest(BaseModel):
    job_id: str
    # Identity fields
    member_number: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    ssn_last4: Optional[str] = None
    dob: Optional[str] = None  # YYYY-MM-DD format
    # Contact fields
    email: Optional[str] = None
    phone: Optional[str] = None
    zip_code: Optional[str] = None
    state: Optional[str] = None
    # Enrollment fields
    status: Optional[str] = None
    carrier_code: Optional[str] = None
    hix_subscriber_id: Optional[str] = None
    hix_member_id: Optional[str] = None
    application_id: Optional[str] = None
    # Date ranges
    effective_date_from: Optional[str] = None
    effective_date_to: Optional[str] = None
    term_date_from: Optional[str] = None
    term_date_to: Optional[str] = None
    # Pagination
    limit: int = 100
    offset: int = 0


class MemberAnalysisRequest(BaseModel):
    job_id: str
    member_id: str


# =========================================================================
# Helpers
# =========================================================================

def get_db_path(job_id: str) -> str:
    """Get database path for a job"""
    db_path = os.path.join(DATABASES_DIR, f"{job_id}.db")
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Database not found")
    return db_path


def get_table_name(job_id: str) -> str:
    """Get the main table name from the database"""
    db_path = get_db_path(job_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = cursor.fetchall()
    conn.close()
    if not tables:
        raise HTTPException(status_code=404, detail="No tables found")
    return tables[0][0]


def get_column_mappings(job_id: str) -> Dict[str, str]:
    """
    Map standard field names to actual column names in the dataset.
    This allows flexible matching across different data sources.
    """
    db_path = get_db_path(job_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    table_name = get_table_name(job_id)
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1].lower() for row in cursor.fetchall()]
    conn.close()
    
    # Define mapping patterns (field_name -> list of possible column name patterns)
    patterns = {
        'member_number': ['member_number', 'member_id', 'memberid', 'member_no', 'memberno', 'cim', 'case_number', 'casenumber'],
        'first_name': ['first_name', 'firstname', 'fname', 'first', 'given_name'],
        'last_name': ['last_name', 'lastname', 'lname', 'last', 'surname', 'family_name'],
        'ssn': ['ssn', 'social_security', 'social', 'ss_number', 'ssnumber'],
        'dob': ['dob', 'date_of_birth', 'dateofbirth', 'birth_date', 'birthdate', 'birthday'],
        'email': ['email', 'email_address', 'emailaddress', 'e_mail'],
        'phone': ['phone', 'phone_number', 'phonenumber', 'telephone', 'mobile', 'cell'],
        'zip_code': ['zip', 'zip_code', 'zipcode', 'postal', 'postal_code'],
        'state': ['state', 'st', 'state_code', 'statecode', 'province'],
        'status': ['status', 'enrollment_status', 'member_status', 'case_status'],
        'carrier_code': ['carrier', 'carrier_code', 'carriercode', 'carrier_id', 'plan_code'],
        'hix_subscriber_id': ['hix_subscriber_id', 'hix_sub_id', 'subscriber_id', 'hix_subscriber'],
        'hix_member_id': ['hix_member_id', 'hix_mem_id', 'hix_member'],
        'application_id': ['application_id', 'app_id', 'applicationid', 'application_number'],
        'effective_date': ['effective_date', 'effectivedate', 'eff_date', 'start_date', 'coverage_start'],
        'term_date': ['term_date', 'termdate', 'termination_date', 'end_date', 'coverage_end'],
        'address': ['address', 'street', 'street_address', 'address_line_1', 'address1'],
        'city': ['city', 'town', 'municipality'],
    }
    
    mappings = {}
    for field, patterns_list in patterns.items():
        for pattern in patterns_list:
            if pattern in columns:
                mappings[field] = pattern
                break
    
    return mappings


def build_search_conditions(
    columns: Dict[str, str],
    request: AdvancedSearchRequest
) -> tuple:
    """Build SQL WHERE conditions from search request"""
    conditions = []
    params = []
    
    # Text field searches (LIKE matching)
    text_fields = [
        ('member_number', request.member_number),
        ('first_name', request.first_name),
        ('last_name', request.last_name),
        ('email', request.email),
        ('phone', request.phone),
        ('zip_code', request.zip_code),
        ('state', request.state),
        ('carrier_code', request.carrier_code),
        ('hix_subscriber_id', request.hix_subscriber_id),
        ('hix_member_id', request.hix_member_id),
        ('application_id', request.application_id),
    ]
    
    for field, value in text_fields:
        if value and field in columns:
            col = columns[field]
            conditions.append(f"LOWER({col}) LIKE LOWER(?)")
            params.append(f"%{value}%")
    
    # SSN last 4 (special handling)
    if request.ssn_last4 and 'ssn' in columns:
        conditions.append(f"{columns['ssn']} LIKE ?")
        params.append(f"%{request.ssn_last4}")
    
    # Status exact match
    if request.status and 'status' in columns:
        conditions.append(f"LOWER({columns['status']}) = LOWER(?)")
        params.append(request.status)
    
    # Date of birth
    if request.dob and 'dob' in columns:
        conditions.append(f"{columns['dob']} = ?")
        params.append(request.dob)
    
    # Date range filters
    if request.effective_date_from and 'effective_date' in columns:
        conditions.append(f"{columns['effective_date']} >= ?")
        params.append(request.effective_date_from)
    if request.effective_date_to and 'effective_date' in columns:
        conditions.append(f"{columns['effective_date']} <= ?")
        params.append(request.effective_date_to)
    if request.term_date_from and 'term_date' in columns:
        conditions.append(f"{columns['term_date']} >= ?")
        params.append(request.term_date_from)
    if request.term_date_to and 'term_date' in columns:
        conditions.append(f"{columns['term_date']} <= ?")
        params.append(request.term_date_to)
    
    return conditions, params


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/schema/{job_id}")
async def get_member_schema(job_id: str):
    """
    Get the schema and field mappings for member data.
    Returns available columns and which standard fields they map to.
    """
    try:
        db_path = get_db_path(job_id)
        table_name = get_table_name(job_id)
        mappings = get_column_mappings(job_id)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        all_columns = [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]
        conn.close()
        
        return {
            "table_name": table_name,
            "columns": all_columns,
            "field_mappings": mappings,
            "searchable_fields": list(mappings.keys())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search")
async def quick_search(request: QuickSearchRequest):
    """
    Quick unified search across common member fields.
    Searches: Name, Member Number, Email, Phone, SSN (last 4)
    """
    try:
        db_path = get_db_path(request.job_id)
        table_name = get_table_name(request.job_id)
        mappings = get_column_mappings(request.job_id)
        
        query_term = request.query.strip()
        if not query_term:
            return {"members": [], "count": 0}
        
        # Build OR conditions for quick search
        conditions = []
        params = []
        
        search_fields = ['first_name', 'last_name', 'member_number', 'email', 'phone', 'ssn']
        for field in search_fields:
            if field in mappings:
                col = mappings[field]
                conditions.append(f"LOWER({col}) LIKE LOWER(?)")
                params.append(f"%{query_term}%")
        
        if not conditions:
            return {"members": [], "count": 0, "message": "No searchable columns found"}
        
        where_clause = " OR ".join(conditions)
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}", params)
        total_count = cursor.fetchone()[0]
        
        # Get results
        cursor.execute(
            f"SELECT rowid as _row_id, * FROM {table_name} WHERE {where_clause} LIMIT ?",
            params + [request.limit]
        )
        rows = cursor.fetchall()
        conn.close()
        
        members = [dict(row) for row in rows]
        
        return {
            "members": members,
            "count": len(members),
            "total_count": total_count,
            "field_mappings": mappings
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/advanced-search")
async def advanced_search(request: AdvancedSearchRequest):
    """
    Advanced multi-field search with filters.
    Supports all member fields including date ranges.
    """
    try:
        db_path = get_db_path(request.job_id)
        table_name = get_table_name(request.job_id)
        mappings = get_column_mappings(request.job_id)
        
        conditions, params = build_search_conditions(mappings, request)
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if conditions:
            where_clause = " AND ".join(conditions)
            count_query = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
            data_query = f"SELECT rowid as _row_id, * FROM {table_name} WHERE {where_clause} LIMIT ? OFFSET ?"
        else:
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            data_query = f"SELECT rowid as _row_id, * FROM {table_name} LIMIT ? OFFSET ?"
            params = []
        
        # Get count
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Get results
        cursor.execute(data_query, params + [request.limit, request.offset])
        rows = cursor.fetchall()
        conn.close()
        
        members = [dict(row) for row in rows]
        
        return {
            "members": members,
            "count": len(members),
            "total_count": total_count,
            "offset": request.offset,
            "limit": request.limit,
            "has_more": (request.offset + len(members)) < total_count,
            "field_mappings": mappings
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{job_id}/{member_id}")
async def get_member_detail(job_id: str, member_id: int):
    """Get detailed information for a specific member"""
    try:
        db_path = get_db_path(job_id)
        table_name = get_table_name(job_id)
        mappings = get_column_mappings(job_id)
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT rowid as _row_id, * FROM {table_name} WHERE rowid = ?", [member_id])
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail="Member not found")
        
        member = dict(row)
        
        # Build structured member profile
        profile = {
            "raw": member,
            "identity": {},
            "contact": {},
            "enrollment": {},
            "dates": {}
        }
        
        # Map to structured sections
        identity_fields = ['member_number', 'first_name', 'last_name', 'ssn', 'dob']
        contact_fields = ['email', 'phone', 'address', 'city', 'state', 'zip_code']
        enrollment_fields = ['status', 'carrier_code', 'hix_subscriber_id', 'hix_member_id', 'application_id']
        date_fields = ['effective_date', 'term_date']
        
        for field in identity_fields:
            if field in mappings:
                profile['identity'][field] = member.get(mappings[field])
        
        for field in contact_fields:
            if field in mappings:
                profile['contact'][field] = member.get(mappings[field])
        
        for field in enrollment_fields:
            if field in mappings:
                profile['enrollment'][field] = member.get(mappings[field])
        
        for field in date_fields:
            if field in mappings:
                profile['dates'][field] = member.get(mappings[field])
        
        return {
            "member_id": member_id,
            "profile": profile,
            "field_mappings": mappings
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ai-analyze")
async def ai_analyze_member(request: MemberAnalysisRequest):
    """
    Use AI to analyze a member and provide insights.
    Powered by Google Gemini.
    """
    try:
        import google.generativeai as genai
        
        # Get member data
        db_path = get_db_path(request.job_id)
        table_name = get_table_name(request.job_id)
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE rowid = ?", [int(request.member_id)])
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail="Member not found")
        
        member_data = dict(row)
        
        # Configure Gemini
        api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
        if not api_key:
            return {
                "success": False,
                "error": "Gemini API key not configured",
                "insights": []
            }
        
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-pro')
        
        # Build analysis prompt
        prompt = f"""Analyze this ACA/health insurance member record and provide insights:

Member Data:
{member_data}

Please provide:
1. A brief member summary (1-2 sentences)
2. Eligibility assessment (likely eligible, needs review, or potential issues)
3. Recommended actions for this member (2-3 specific suggestions)
4. Risk factors to consider (if any)
5. Outreach recommendation (best channel and timing)

Format your response as JSON with these keys:
- summary: string
- eligibility: "eligible" | "needs_review" | "potential_issues"
- eligibility_notes: string
- recommended_actions: array of strings
- risk_factors: array of strings
- outreach: {{ channel: string, timing: string, message_tone: string }}
"""
        
        response = model.generate_content(prompt)
        
        # Parse response
        import json
        try:
            # Try to extract JSON from response
            text = response.text
            # Find JSON in response
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                insights = json.loads(json_match.group())
            else:
                insights = {"summary": text, "raw_response": True}
        except:
            insights = {"summary": response.text, "parse_error": True}
        
        return {
            "success": True,
            "member_id": request.member_id,
            "insights": insights
        }
        
    except HTTPException:
        raise
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "insights": []
        }


@router.get("/stats/{job_id}")
async def get_member_stats(job_id: str):
    """Get aggregate statistics for member data"""
    try:
        db_path = get_db_path(job_id)
        table_name = get_table_name(job_id)
        mappings = get_column_mappings(job_id)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Total count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cursor.fetchone()[0]
        
        stats = {
            "total_members": total,
            "by_status": {},
            "by_state": {},
            "by_carrier": {}
        }
        
        # Status breakdown
        if 'status' in mappings:
            cursor.execute(f"SELECT {mappings['status']}, COUNT(*) FROM {table_name} GROUP BY {mappings['status']} ORDER BY COUNT(*) DESC LIMIT 10")
            stats['by_status'] = {row[0] or 'Unknown': row[1] for row in cursor.fetchall()}
        
        # State breakdown
        if 'state' in mappings:
            cursor.execute(f"SELECT {mappings['state']}, COUNT(*) FROM {table_name} GROUP BY {mappings['state']} ORDER BY COUNT(*) DESC LIMIT 10")
            stats['by_state'] = {row[0] or 'Unknown': row[1] for row in cursor.fetchall()}
        
        # Carrier breakdown
        if 'carrier_code' in mappings:
            cursor.execute(f"SELECT {mappings['carrier_code']}, COUNT(*) FROM {table_name} GROUP BY {mappings['carrier_code']} ORDER BY COUNT(*) DESC LIMIT 10")
            stats['by_carrier'] = {row[0] or 'Unknown': row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
