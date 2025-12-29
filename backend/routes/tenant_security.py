"""
Tenant Isolation & Security Routes for ACA DataHub
Row-level security, data classification, access management, and compliance
"""

from fastapi import APIRouter, HTTPException, Query, Header
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import hashlib

router = APIRouter(prefix="/tenant-security", tags=["Tenant Security"])


# =========================================================================
# Models
# =========================================================================

class DataClassification(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"


class AccessLevel(str, Enum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


# =========================================================================
# Row-Level Security
# =========================================================================

class RowLevelSecurity:
    """Implement row-level security policies"""
    
    def __init__(self):
        self.policies: Dict[str, dict] = {}
        self._counter = 0
        self._init_policies()
    
    def _init_policies(self):
        self.policies = {
            "tenant_isolation": {
                "id": "tenant_isolation",
                "name": "Tenant Data Isolation",
                "table": "*",
                "condition": "tenant_id = :current_tenant",
                "enabled": True,
                "priority": 1
            },
            "own_data_only": {
                "id": "own_data_only",
                "name": "Own Data Access",
                "table": "leads",
                "condition": "owner_id = :current_user OR org_id = :current_org",
                "enabled": True,
                "priority": 2
            }
        }
    
    def create_policy(
        self,
        name: str,
        table: str,
        condition: str,
        description: str = None
    ) -> dict:
        self._counter += 1
        policy_id = f"rls_{self._counter}"
        
        policy = {
            "id": policy_id,
            "name": name,
            "table": table,
            "condition": condition,
            "description": description or "",
            "enabled": True,
            "priority": len(self.policies) + 1,
            "created_at": datetime.utcnow().isoformat()
        }
        
        self.policies[policy_id] = policy
        return policy
    
    def evaluate_access(
        self,
        table: str,
        user_context: dict,
        row_data: dict
    ) -> dict:
        """Evaluate if user can access a specific row"""
        applicable_policies = []
        
        for policy in self.policies.values():
            if not policy["enabled"]:
                continue
            if policy["table"] == "*" or policy["table"] == table:
                applicable_policies.append(policy)
        
        # Sort by priority
        applicable_policies.sort(key=lambda p: p["priority"])
        
        # Simulate policy evaluation
        allowed = True
        applied_policies = []
        
        for policy in applicable_policies:
            # Simple simulation
            if "tenant_id" in policy["condition"]:
                row_tenant = row_data.get("tenant_id", "default")
                user_tenant = user_context.get("tenant_id", "default")
                policy_result = row_tenant == user_tenant
            else:
                policy_result = True
            
            applied_policies.append({
                "policy_id": policy["id"],
                "result": "allow" if policy_result else "deny"
            })
            
            if not policy_result:
                allowed = False
                break
        
        return {
            "allowed": allowed,
            "table": table,
            "policies_evaluated": len(applicable_policies),
            "applied_policies": applied_policies,
            "evaluated_at": datetime.utcnow().isoformat()
        }


rls = RowLevelSecurity()


# =========================================================================
# Column-Level Encryption
# =========================================================================

class ColumnEncryption:
    """Manage column-level encryption"""
    
    def __init__(self):
        self.encrypted_columns: Dict[str, dict] = {}
        self.keys: Dict[str, dict] = {}
        self._counter = 0
    
    def register_encrypted_column(
        self,
        table: str,
        column: str,
        encryption_type: str = "AES-256",
        key_id: str = None
    ) -> dict:
        self._counter += 1
        col_id = f"{table}.{column}"
        
        record = {
            "id": col_id,
            "table": table,
            "column": column,
            "encryption_type": encryption_type,
            "key_id": key_id or f"key_{self._counter}",
            "registered_at": datetime.utcnow().isoformat()
        }
        
        self.encrypted_columns[col_id] = record
        return record
    
    def encrypt_value(self, value: str, column_id: str) -> dict:
        """Encrypt a value (simulated)"""
        if column_id not in self.encrypted_columns:
            return {"error": "Column not registered for encryption"}
        
        encrypted = hashlib.sha256(str(value).encode()).hexdigest()[:32]
        
        return {
            "column_id": column_id,
            "encrypted_value": f"ENC:{encrypted}",
            "key_id": self.encrypted_columns[column_id]["key_id"]
        }
    
    def decrypt_value(self, encrypted_value: str, column_id: str, user_context: dict) -> dict:
        """Decrypt a value (simulated)"""
        if column_id not in self.encrypted_columns:
            return {"error": "Column not registered"}
        
        # Check if user has permission to decrypt
        has_permission = user_context.get("role") in ["admin", "data_owner"]
        
        if not has_permission:
            return {
                "error": "Access denied",
                "column_id": column_id,
                "masked_value": "****ENCRYPTED****"
            }
        
        return {
            "column_id": column_id,
            "decrypted_value": "[DECRYPTED_VALUE]",
            "decrypted_at": datetime.utcnow().isoformat()
        }


encryption = ColumnEncryption()


# =========================================================================
# Data Classification
# =========================================================================

class DataClassifier:
    """Classify and tag sensitive data"""
    
    def __init__(self):
        self.classifications: Dict[str, dict] = {}
        self.auto_rules: List[dict] = []
        self._init_rules()
    
    def _init_rules(self):
        self.auto_rules = [
            {
                "pattern": "ssn|social_security",
                "classification": DataClassification.PII.value,
                "confidence": 0.95
            },
            {
                "pattern": "email|phone|address",
                "classification": DataClassification.PII.value,
                "confidence": 0.9
            },
            {
                "pattern": "password|secret|api_key",
                "classification": DataClassification.RESTRICTED.value,
                "confidence": 0.99
            },
            {
                "pattern": "salary|income|credit",
                "classification": DataClassification.CONFIDENTIAL.value,
                "confidence": 0.85
            }
        ]
    
    def classify_column(
        self,
        table: str,
        column: str,
        classification: str = None
    ) -> dict:
        """Classify a column"""
        col_id = f"{table}.{column}"
        
        if classification:
            auto_detected = False
            confidence = 1.0
        else:
            # Auto-detect classification
            classification, confidence = self._auto_classify(column)
            auto_detected = True
        
        record = {
            "id": col_id,
            "table": table,
            "column": column,
            "classification": classification,
            "auto_detected": auto_detected,
            "confidence": confidence,
            "classified_at": datetime.utcnow().isoformat()
        }
        
        self.classifications[col_id] = record
        return record
    
    def _auto_classify(self, column: str) -> tuple:
        column_lower = column.lower()
        
        for rule in self.auto_rules:
            if any(p in column_lower for p in rule["pattern"].split("|")):
                return rule["classification"], rule["confidence"]
        
        return DataClassification.INTERNAL.value, 0.5
    
    def scan_table(self, table: str, columns: List[str]) -> dict:
        """Scan table for sensitive data"""
        results = []
        
        for column in columns:
            classification = self.classify_column(table, column)
            results.append(classification)
        
        return {
            "table": table,
            "columns_scanned": len(columns),
            "classifications": results,
            "pii_columns": [r for r in results if r["classification"] == DataClassification.PII.value],
            "scanned_at": datetime.utcnow().isoformat()
        }


classifier = DataClassifier()


# =========================================================================
# Access Request Manager
# =========================================================================

class AccessRequestManager:
    """Manage access requests and temporary elevation"""
    
    def __init__(self):
        self.requests: Dict[str, dict] = {}
        self.elevated_access: Dict[str, dict] = {}
        self._counter = 0
    
    def request_access(
        self,
        user_id: str,
        resource: str,
        access_level: str,
        reason: str,
        duration_hours: int = 4
    ) -> dict:
        self._counter += 1
        request_id = f"req_{self._counter}"
        
        request = {
            "id": request_id,
            "user_id": user_id,
            "resource": resource,
            "access_level": access_level,
            "reason": reason,
            "duration_hours": duration_hours,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": None,
            "approved_by": None
        }
        
        self.requests[request_id] = request
        return request
    
    def approve_request(self, request_id: str, approver_id: str) -> dict:
        if request_id not in self.requests:
            raise ValueError("Request not found")
        
        request = self.requests[request_id]
        request["status"] = "approved"
        request["approved_by"] = approver_id
        request["approved_at"] = datetime.utcnow().isoformat()
        request["expires_at"] = (
            datetime.utcnow() + timedelta(hours=request["duration_hours"])
        ).isoformat()
        
        # Grant elevated access
        self.elevated_access[request["user_id"]] = {
            "request_id": request_id,
            "resource": request["resource"],
            "access_level": request["access_level"],
            "expires_at": request["expires_at"]
        }
        
        return request
    
    def deny_request(self, request_id: str, reason: str) -> dict:
        if request_id not in self.requests:
            raise ValueError("Request not found")
        
        request = self.requests[request_id]
        request["status"] = "denied"
        request["denial_reason"] = reason
        
        return request
    
    def check_elevated_access(self, user_id: str) -> dict:
        if user_id not in self.elevated_access:
            return {"has_elevated_access": False}
        
        access = self.elevated_access[user_id]
        now = datetime.utcnow().isoformat()
        
        if access["expires_at"] < now:
            del self.elevated_access[user_id]
            return {"has_elevated_access": False, "expired": True}
        
        return {
            "has_elevated_access": True,
            "resource": access["resource"],
            "access_level": access["access_level"],
            "expires_at": access["expires_at"]
        }


access_manager = AccessRequestManager()


# =========================================================================
# Security Posture
# =========================================================================

class SecurityPosture:
    """Calculate and track security posture"""
    
    def calculate_score(self) -> dict:
        """Calculate overall security posture score"""
        scores = {
            "rls_coverage": random.randint(70, 100),
            "encryption_coverage": random.randint(60, 95),
            "data_classification": random.randint(75, 100),
            "access_control": random.randint(80, 100),
            "audit_logging": random.randint(85, 100)
        }
        
        overall = sum(scores.values()) / len(scores)
        
        recommendations = []
        if scores["rls_coverage"] < 80:
            recommendations.append("Increase row-level security policy coverage")
        if scores["encryption_coverage"] < 80:
            recommendations.append("Encrypt additional sensitive columns")
        if scores["data_classification"] < 80:
            recommendations.append("Complete data classification for all tables")
        
        return {
            "overall_score": round(overall, 1),
            "grade": self._score_to_grade(overall),
            "dimension_scores": scores,
            "recommendations": recommendations,
            "calculated_at": datetime.utcnow().isoformat()
        }
    
    def _score_to_grade(self, score: float) -> str:
        if score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        else:
            return "F"


posture = SecurityPosture()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/rls/policies")
async def list_rls_policies():
    """List row-level security policies"""
    return {"policies": list(rls.policies.values())}


@router.post("/rls/policies")
async def create_rls_policy(
    name: str = Query(...),
    table: str = Query(...),
    condition: str = Query(...)
):
    """Create row-level security policy"""
    return rls.create_policy(name, table, condition)


@router.post("/rls/evaluate")
async def evaluate_row_access(
    table: str = Query(...),
    user_context: dict = None,
    row_data: dict = None
):
    """Evaluate row-level access"""
    return rls.evaluate_access(table, user_context or {}, row_data or {})


@router.post("/encryption/columns")
async def register_encrypted_column(
    table: str = Query(...),
    column: str = Query(...),
    encryption_type: str = Query(default="AES-256")
):
    """Register column for encryption"""
    return encryption.register_encrypted_column(table, column, encryption_type)


@router.get("/encryption/columns")
async def list_encrypted_columns():
    """List encrypted columns"""
    return {"columns": list(encryption.encrypted_columns.values())}


@router.post("/classification/classify")
async def classify_column(
    table: str = Query(...),
    column: str = Query(...),
    classification: Optional[DataClassification] = Query(default=None)
):
    """Classify a column"""
    cls_value = classification.value if classification else None
    return classifier.classify_column(table, column, cls_value)


@router.post("/classification/scan")
async def scan_table(table: str = Query(...), columns: List[str] = Query(...)):
    """Scan table for sensitive data"""
    return classifier.scan_table(table, columns)


@router.get("/classifications")
async def list_classifications():
    """List all data classifications"""
    return {"classifications": list(classifier.classifications.values())}


@router.post("/access-requests")
async def request_access(
    user_id: str = Query(...),
    resource: str = Query(...),
    access_level: AccessLevel = Query(...),
    reason: str = Query(...),
    duration_hours: int = Query(default=4)
):
    """Request elevated access"""
    return access_manager.request_access(
        user_id, resource, access_level.value, reason, duration_hours
    )


@router.get("/access-requests")
async def list_access_requests(status: Optional[str] = Query(default=None)):
    """List access requests"""
    requests = list(access_manager.requests.values())
    if status:
        requests = [r for r in requests if r["status"] == status]
    return {"requests": requests}


@router.post("/access-requests/{request_id}/approve")
async def approve_request(request_id: str, approver_id: str = Query(...)):
    """Approve access request"""
    try:
        return access_manager.approve_request(request_id, approver_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/access-requests/{request_id}/deny")
async def deny_request(request_id: str, reason: str = Query(...)):
    """Deny access request"""
    try:
        return access_manager.deny_request(request_id, reason)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/elevated-access/{user_id}")
async def check_elevated_access(user_id: str):
    """Check user's elevated access"""
    return access_manager.check_elevated_access(user_id)


@router.get("/posture")
async def get_security_posture():
    """Get security posture score"""
    return posture.calculate_score()
