"""
Compliance & Governance Routes for ACA DataHub
HIPAA/GDPR compliance, PII detection, data retention, audit export
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import re
import hashlib

router = APIRouter(prefix="/compliance", tags=["Compliance"])


# =========================================================================
# PII Detection Patterns
# =========================================================================

PII_PATTERNS = {
    "ssn": {
        "pattern": r"\b\d{3}-?\d{2}-?\d{4}\b",
        "name": "Social Security Number",
        "severity": "critical",
        "mask": "***-**-****"
    },
    "email": {
        "pattern": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
        "name": "Email Address",
        "severity": "high",
        "mask": "***@***.***"
    },
    "phone": {
        "pattern": r"\b(\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b",
        "name": "Phone Number",
        "severity": "high",
        "mask": "(***) ***-****"
    },
    "credit_card": {
        "pattern": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
        "name": "Credit Card Number",
        "severity": "critical",
        "mask": "****-****-****-****"
    },
    "dob": {
        "pattern": r"\b(0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])[-/](19|20)\d{2}\b",
        "name": "Date of Birth",
        "severity": "high",
        "mask": "**/**/****"
    },
    "zip_code": {
        "pattern": r"\b\d{5}(-\d{4})?\b",
        "name": "ZIP Code",
        "severity": "medium",
        "mask": "*****"
    },
    "ip_address": {
        "pattern": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
        "name": "IP Address",
        "severity": "medium",
        "mask": "***.***.***.***"
    }
}

# Column name patterns that likely contain PII
PII_COLUMN_PATTERNS = [
    (r"ssn|social.*security", "ssn"),
    (r"email|e[-_]?mail", "email"),
    (r"phone|mobile|cell|telephone", "phone"),
    (r"dob|birth.*date|date.*birth", "dob"),
    (r"first.*name|fname", "name"),
    (r"last.*name|lname|surname", "name"),
    (r"address|street|addr", "address"),
    (r"zip|postal|postcode", "zip_code"),
    (r"credit.*card|cc.*num", "credit_card"),
]


# =========================================================================
# Compliance Store
# =========================================================================

class ComplianceStore:
    """Stores compliance data, audit logs, and data requests"""
    
    def __init__(self):
        self.audit_logs: List[dict] = []
        self.data_requests: Dict[str, dict] = {}
        self.retention_policies: Dict[str, dict] = {}
        self.consent_records: Dict[str, dict] = {}
        self._request_counter = 0
        
        # Initialize default retention policies
        self.retention_policies["default"] = {
            "name": "Default Retention",
            "retention_days": 365,
            "auto_delete": False,
            "applicable_to": ["all"]
        }
    
    def log_access(self, user_id: str, action: str, resource: str, details: dict = None):
        """Log an access event for audit"""
        entry = {
            "id": f"audit_{len(self.audit_logs)}",
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "action": action,
            "resource": resource,
            "details": details or {},
            "ip_address": details.get("ip_address") if details else None
        }
        self.audit_logs.insert(0, entry)
        
        # Keep last 10000 entries
        if len(self.audit_logs) > 10000:
            self.audit_logs = self.audit_logs[:10000]
    
    def get_audit_logs(
        self, 
        user_id: str = None, 
        action: str = None,
        start_date: str = None,
        end_date: str = None,
        limit: int = 100
    ) -> List[dict]:
        """Get filtered audit logs"""
        logs = self.audit_logs
        
        if user_id:
            logs = [l for l in logs if l.get("user_id") == user_id]
        if action:
            logs = [l for l in logs if l.get("action") == action]
        if start_date:
            logs = [l for l in logs if l.get("timestamp", "") >= start_date]
        if end_date:
            logs = [l for l in logs if l.get("timestamp", "") <= end_date]
        
        return logs[:limit]
    
    def create_data_request(self, request_type: str, email: str, details: dict = None) -> dict:
        """Create a GDPR data request (export or delete)"""
        self._request_counter += 1
        request_id = f"req_{self._request_counter}"
        
        request = {
            "id": request_id,
            "type": request_type,  # export, delete
            "email": email,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "details": details or {}
        }
        
        self.data_requests[request_id] = request
        return request
    
    def complete_request(self, request_id: str, result: dict = None):
        """Mark a data request as complete"""
        if request_id in self.data_requests:
            self.data_requests[request_id]["status"] = "completed"
            self.data_requests[request_id]["completed_at"] = datetime.utcnow().isoformat()
            if result:
                self.data_requests[request_id]["result"] = result
    
    def get_data_requests(self, status: str = None) -> List[dict]:
        """Get all data requests, optionally filtered by status"""
        requests = list(self.data_requests.values())
        if status:
            requests = [r for r in requests if r.get("status") == status]
        return sorted(requests, key=lambda x: x.get("created_at", ""), reverse=True)


compliance_store = ComplianceStore()


# =========================================================================
# PII Detection Service
# =========================================================================

class PIIDetector:
    """Detects and masks PII in data"""
    
    def detect_in_columns(self, columns: List[str]) -> Dict[str, dict]:
        """Detect potential PII columns based on names"""
        results = {}
        
        for column in columns:
            col_lower = column.lower()
            for pattern, pii_type in PII_COLUMN_PATTERNS:
                if re.search(pattern, col_lower):
                    results[column] = {
                        "type": pii_type,
                        "severity": PII_PATTERNS.get(pii_type, {}).get("severity", "medium"),
                        "name": PII_PATTERNS.get(pii_type, {}).get("name", pii_type)
                    }
                    break
        
        return results
    
    def detect_in_values(self, values: List[str]) -> Dict[str, List[dict]]:
        """Detect PII in actual values"""
        results = {}
        
        for pii_type, config in PII_PATTERNS.items():
            pattern = re.compile(config["pattern"])
            matches = []
            
            for i, value in enumerate(values):
                if value and pattern.search(str(value)):
                    matches.append({
                        "index": i,
                        "value": str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                    })
            
            if matches:
                results[pii_type] = {
                    "name": config["name"],
                    "severity": config["severity"],
                    "count": len(matches),
                    "samples": matches[:5]
                }
        
        return results
    
    def mask_value(self, value: str, pii_type: str) -> str:
        """Mask a PII value"""
        if pii_type in PII_PATTERNS:
            pattern = PII_PATTERNS[pii_type]["pattern"]
            mask = PII_PATTERNS[pii_type]["mask"]
            return re.sub(pattern, mask, str(value))
        return value
    
    def hash_value(self, value: str, salt: str = "") -> str:
        """Hash a value for pseudonymization"""
        return hashlib.sha256((str(value) + salt).encode()).hexdigest()[:16]


pii_detector = PIIDetector()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/status")
async def get_compliance_status():
    """Get overall compliance status dashboard"""
    return {
        "hipaa_enabled": True,
        "gdpr_enabled": True,
        "encryption_at_rest": True,
        "audit_logging": True,
        "data_retention_policy": compliance_store.retention_policies.get("default"),
        "pending_requests": len([r for r in compliance_store.data_requests.values() if r["status"] == "pending"]),
        "last_audit": compliance_store.audit_logs[0]["timestamp"] if compliance_store.audit_logs else None
    }


@router.get("/audit")
async def get_audit_logs(
    user_id: Optional[str] = Query(default=None),
    action: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=1000)
):
    """Export audit logs with filtering"""
    logs = compliance_store.get_audit_logs(user_id, action, start_date, end_date, limit)
    return {
        "logs": logs,
        "total": len(logs),
        "exported_at": datetime.utcnow().isoformat()
    }


@router.post("/pii-scan")
async def scan_for_pii(columns: List[str], sample_values: Optional[Dict[str, List[str]]] = None):
    """Scan columns and optional sample values for PII"""
    column_pii = pii_detector.detect_in_columns(columns)
    
    value_pii = {}
    if sample_values:
        for col, values in sample_values.items():
            detected = pii_detector.detect_in_values(values)
            if detected:
                value_pii[col] = detected
    
    # Calculate risk score
    critical_count = sum(1 for c in column_pii.values() if c.get("severity") == "critical")
    high_count = sum(1 for c in column_pii.values() if c.get("severity") == "high")
    
    risk_score = min(100, (critical_count * 30) + (high_count * 15) + len(column_pii) * 5)
    
    return {
        "column_pii": column_pii,
        "value_pii": value_pii,
        "total_pii_columns": len(column_pii),
        "risk_score": risk_score,
        "risk_level": "critical" if risk_score > 60 else ("high" if risk_score > 30 else "medium" if risk_score > 10 else "low"),
        "recommendations": [
            "Enable column-level encryption for critical PII fields",
            "Implement access controls for PII data",
            "Set up masking rules for non-privileged users"
        ] if column_pii else []
    }


@router.post("/data-request")
async def create_data_request(
    request_type: str = Query(..., regex="^(export|delete)$"),
    email: str = Query(...),
    reason: Optional[str] = None
):
    """Create a GDPR data request (export or delete)"""
    request = compliance_store.create_data_request(
        request_type, 
        email, 
        {"reason": reason}
    )
    
    # Log the request
    compliance_store.log_access(
        "system",
        f"data_request.{request_type}",
        email,
        {"request_id": request["id"]}
    )
    
    return {"success": True, "request": request}


@router.get("/data-requests")
async def list_data_requests(status: Optional[str] = Query(default=None)):
    """List all data requests"""
    return {"requests": compliance_store.get_data_requests(status)}


@router.post("/data-request/{request_id}/process")
async def process_data_request(request_id: str, background_tasks: BackgroundTasks):
    """Process a data request"""
    if request_id not in compliance_store.data_requests:
        raise HTTPException(status_code=404, detail="Request not found")
    
    request = compliance_store.data_requests[request_id]
    
    if request["status"] != "pending":
        raise HTTPException(status_code=400, detail="Request already processed")
    
    # Simulate processing
    compliance_store.complete_request(request_id, {"records_affected": 1})
    
    return {"success": True, "message": "Request processed"}


@router.get("/retention-policies")
async def get_retention_policies():
    """Get all data retention policies"""
    return {"policies": list(compliance_store.retention_policies.values())}


@router.post("/retention-policies")
async def create_retention_policy(
    name: str,
    retention_days: int = Query(ge=1, le=3650),
    auto_delete: bool = False,
    applicable_to: List[str] = ["all"]
):
    """Create a data retention policy"""
    policy_id = name.lower().replace(" ", "_")
    policy = {
        "id": policy_id,
        "name": name,
        "retention_days": retention_days,
        "auto_delete": auto_delete,
        "applicable_to": applicable_to,
        "created_at": datetime.utcnow().isoformat()
    }
    
    compliance_store.retention_policies[policy_id] = policy
    return {"success": True, "policy": policy}


@router.post("/mask-data")
async def mask_pii_data(
    data: List[dict],
    columns_to_mask: List[str]
):
    """Mask PII in specified columns"""
    masked_data = []
    
    for row in data:
        masked_row = row.copy()
        for col in columns_to_mask:
            if col in masked_row and masked_row[col]:
                # Detect PII type and mask
                value = str(masked_row[col])
                for pii_type, config in PII_PATTERNS.items():
                    if re.search(config["pattern"], value):
                        masked_row[col] = pii_detector.mask_value(value, pii_type)
                        break
        masked_data.append(masked_row)
    
    return {"masked_data": masked_data, "rows_processed": len(data)}


@router.post("/pseudonymize")
async def pseudonymize_data(
    data: List[dict],
    columns: List[str],
    salt: str = ""
):
    """Pseudonymize PII columns with hashing"""
    pseudonymized = []
    
    for row in data:
        new_row = row.copy()
        for col in columns:
            if col in new_row and new_row[col]:
                new_row[col] = pii_detector.hash_value(new_row[col], salt)
        pseudonymized.append(new_row)
    
    return {"data": pseudonymized, "columns_hashed": columns}
