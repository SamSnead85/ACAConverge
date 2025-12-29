"""
Data Contracts Routes for ACA DataHub
Schema evolution, breaking change detection, contract versioning, and SLA management
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import hashlib
import json

router = APIRouter(prefix="/contracts", tags=["Data Contracts"])


# =========================================================================
# Models
# =========================================================================

class CompatibilityMode(str, Enum):
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"
    NONE = "none"


class ContractStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class ChangeType(str, Enum):
    COMPATIBLE = "compatible"
    BREAKING = "breaking"
    UNKNOWN = "unknown"


# =========================================================================
# Schema Registry
# =========================================================================

class SchemaRegistry:
    """Manage data schemas and evolution"""
    
    def __init__(self):
        self.schemas: Dict[str, List[dict]] = {}
        self.contracts: Dict[str, dict] = {}
        self.consumers: Dict[str, List[dict]] = {}
        self._counter = 0
    
    def register_schema(
        self,
        subject: str,
        schema: dict,
        compatibility: str = "backward"
    ) -> dict:
        self._counter += 1
        version = len(self.schemas.get(subject, [])) + 1
        
        schema_record = {
            "id": f"schema_{self._counter}",
            "subject": subject,
            "version": version,
            "schema": schema,
            "schema_hash": hashlib.md5(json.dumps(schema, sort_keys=True).encode()).hexdigest(),
            "compatibility": compatibility,
            "registered_at": datetime.utcnow().isoformat()
        }
        
        if subject not in self.schemas:
            self.schemas[subject] = []
        
        self.schemas[subject].append(schema_record)
        return schema_record
    
    def get_latest_schema(self, subject: str) -> Optional[dict]:
        versions = self.schemas.get(subject, [])
        return versions[-1] if versions else None
    
    def get_schema_versions(self, subject: str) -> List[dict]:
        return self.schemas.get(subject, [])
    
    def check_compatibility(self, subject: str, new_schema: dict) -> dict:
        """Check if new schema is compatible with existing"""
        existing = self.get_latest_schema(subject)
        
        if not existing:
            return {
                "compatible": True,
                "change_type": ChangeType.COMPATIBLE.value,
                "issues": []
            }
        
        old_schema = existing["schema"]
        compatibility_mode = existing.get("compatibility", "backward")
        
        issues = []
        
        # Check for removed fields (breaking in backward mode)
        old_fields = set(old_schema.get("fields", {}).keys())
        new_fields = set(new_schema.get("fields", {}).keys())
        
        removed = old_fields - new_fields
        if removed and compatibility_mode in ["backward", "full"]:
            issues.append({
                "type": "breaking",
                "category": "field_removed",
                "details": f"Fields removed: {list(removed)}"
            })
        
        # Check for type changes
        for field in old_fields & new_fields:
            old_type = old_schema.get("fields", {}).get(field, {}).get("type")
            new_type = new_schema.get("fields", {}).get(field, {}).get("type")
            
            if old_type != new_type:
                issues.append({
                    "type": "breaking",
                    "category": "type_change",
                    "details": f"Field '{field}' type changed from {old_type} to {new_type}"
                })
        
        # Check for new required fields
        added = new_fields - old_fields
        for field in added:
            if new_schema.get("fields", {}).get(field, {}).get("required"):
                issues.append({
                    "type": "breaking",
                    "category": "required_field_added",
                    "details": f"New required field: {field}"
                })
        
        is_compatible = all(i["type"] != "breaking" for i in issues)
        
        return {
            "compatible": is_compatible,
            "change_type": ChangeType.COMPATIBLE.value if is_compatible else ChangeType.BREAKING.value,
            "compatibility_mode": compatibility_mode,
            "issues": issues
        }


schema_registry = SchemaRegistry()


# =========================================================================
# Contract Manager
# =========================================================================

class ContractManager:
    """Manage data contracts between producers and consumers"""
    
    def __init__(self):
        self.contracts: Dict[str, dict] = {}
        self.slas: Dict[str, dict] = {}
        self.notifications: List[dict] = []
        self._counter = 0
    
    def create_contract(
        self,
        name: str,
        producer: str,
        schema_subject: str,
        description: str = None,
        sla: dict = None
    ) -> dict:
        self._counter += 1
        contract_id = f"contract_{self._counter}"
        
        contract = {
            "id": contract_id,
            "name": name,
            "producer": producer,
            "schema_subject": schema_subject,
            "description": description or "",
            "status": ContractStatus.DRAFT.value,
            "version": 1,
            "sla": sla or {},
            "consumers": [],
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        self.contracts[contract_id] = contract
        return contract
    
    def add_consumer(self, contract_id: str, consumer_id: str, consumer_name: str) -> dict:
        if contract_id not in self.contracts:
            raise ValueError("Contract not found")
        
        contract = self.contracts[contract_id]
        
        consumer = {
            "consumer_id": consumer_id,
            "consumer_name": consumer_name,
            "subscribed_at": datetime.utcnow().isoformat(),
            "schema_version": len(schema_registry.schemas.get(contract["schema_subject"], []))
        }
        
        contract["consumers"].append(consumer)
        return consumer
    
    def activate_contract(self, contract_id: str) -> dict:
        if contract_id not in self.contracts:
            raise ValueError("Contract not found")
        
        contract = self.contracts[contract_id]
        contract["status"] = ContractStatus.ACTIVE.value
        contract["activated_at"] = datetime.utcnow().isoformat()
        return contract
    
    def deprecate_contract(self, contract_id: str, sunset_date: str = None) -> dict:
        if contract_id not in self.contracts:
            raise ValueError("Contract not found")
        
        contract = self.contracts[contract_id]
        contract["status"] = ContractStatus.DEPRECATED.value
        contract["deprecated_at"] = datetime.utcnow().isoformat()
        contract["sunset_date"] = sunset_date or (datetime.utcnow() + timedelta(days=90)).isoformat()
        
        # Notify consumers
        for consumer in contract["consumers"]:
            self.notifications.append({
                "contract_id": contract_id,
                "consumer_id": consumer["consumer_id"],
                "type": "deprecation_notice",
                "message": f"Contract {contract['name']} is deprecated. Sunset: {contract['sunset_date']}",
                "created_at": datetime.utcnow().isoformat()
            })
        
        return contract
    
    def set_sla(
        self,
        contract_id: str,
        freshness_minutes: int = None,
        availability_percent: float = None,
        completeness_percent: float = None
    ) -> dict:
        if contract_id not in self.contracts:
            raise ValueError("Contract not found")
        
        sla = {
            "freshness_minutes": freshness_minutes,
            "availability_percent": availability_percent,
            "completeness_percent": completeness_percent,
            "set_at": datetime.utcnow().isoformat()
        }
        
        self.contracts[contract_id]["sla"] = sla
        self.slas[contract_id] = sla
        return sla
    
    def check_sla_compliance(self, contract_id: str) -> dict:
        if contract_id not in self.contracts:
            raise ValueError("Contract not found")
        
        sla = self.contracts[contract_id].get("sla", {})
        
        # Simulate SLA checks
        checks = []
        overall_compliant = True
        
        if sla.get("freshness_minutes"):
            actual_freshness = random.randint(1, sla["freshness_minutes"] + 30)
            compliant = actual_freshness <= sla["freshness_minutes"]
            overall_compliant = overall_compliant and compliant
            checks.append({
                "metric": "freshness",
                "target": f"{sla['freshness_minutes']} minutes",
                "actual": f"{actual_freshness} minutes",
                "compliant": compliant
            })
        
        if sla.get("availability_percent"):
            actual_availability = round(random.uniform(95, 100), 2)
            compliant = actual_availability >= sla["availability_percent"]
            overall_compliant = overall_compliant and compliant
            checks.append({
                "metric": "availability",
                "target": f"{sla['availability_percent']}%",
                "actual": f"{actual_availability}%",
                "compliant": compliant
            })
        
        if sla.get("completeness_percent"):
            actual_completeness = round(random.uniform(90, 100), 2)
            compliant = actual_completeness >= sla["completeness_percent"]
            overall_compliant = overall_compliant and compliant
            checks.append({
                "metric": "completeness",
                "target": f"{sla['completeness_percent']}%",
                "actual": f"{actual_completeness}%",
                "compliant": compliant
            })
        
        return {
            "contract_id": contract_id,
            "checked_at": datetime.utcnow().isoformat(),
            "overall_compliant": overall_compliant,
            "checks": checks
        }
    
    def get_dependency_graph(self) -> dict:
        """Build dependency graph of contracts"""
        nodes = []
        edges = []
        
        for contract_id, contract in self.contracts.items():
            nodes.append({
                "id": contract_id,
                "name": contract["name"],
                "type": "producer",
                "producer": contract["producer"]
            })
            
            for consumer in contract["consumers"]:
                edges.append({
                    "source": contract_id,
                    "target": consumer["consumer_id"],
                    "type": "consumes"
                })
        
        return {"nodes": nodes, "edges": edges}


contract_manager = ContractManager()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/schemas")
async def register_schema(
    subject: str = Query(...),
    schema: dict = None,
    compatibility: CompatibilityMode = Query(default=CompatibilityMode.BACKWARD)
):
    """Register a new schema version"""
    schema = schema or {"type": "record", "fields": {}}
    record = schema_registry.register_schema(subject, schema, compatibility.value)
    return {"success": True, "schema": record}


@router.get("/schemas/{subject}")
async def get_schema(subject: str, version: Optional[int] = Query(default=None)):
    """Get schema by subject"""
    if version:
        versions = schema_registry.get_schema_versions(subject)
        schema = next((v for v in versions if v["version"] == version), None)
    else:
        schema = schema_registry.get_latest_schema(subject)
    
    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    return schema


@router.get("/schemas/{subject}/versions")
async def get_schema_versions(subject: str):
    """Get all versions of a schema"""
    return {"versions": schema_registry.get_schema_versions(subject)}


@router.post("/schemas/{subject}/compatibility")
async def check_schema_compatibility(subject: str, new_schema: dict):
    """Check schema compatibility"""
    return schema_registry.check_compatibility(subject, new_schema)


@router.post("/")
async def create_contract(
    name: str = Query(...),
    producer: str = Query(...),
    schema_subject: str = Query(...),
    description: str = Query(default="")
):
    """Create a new data contract"""
    contract = contract_manager.create_contract(name, producer, schema_subject, description)
    return {"success": True, "contract": contract}


@router.get("/")
async def list_contracts(status: Optional[str] = Query(default=None)):
    """List data contracts"""
    contracts = list(contract_manager.contracts.values())
    if status:
        contracts = [c for c in contracts if c["status"] == status]
    return {"contracts": contracts}


@router.get("/{contract_id}")
async def get_contract(contract_id: str):
    """Get contract details"""
    contract = contract_manager.contracts.get(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    return contract


@router.post("/{contract_id}/consumers")
async def add_consumer(
    contract_id: str,
    consumer_id: str = Query(...),
    consumer_name: str = Query(...)
):
    """Add consumer to contract"""
    try:
        consumer = contract_manager.add_consumer(contract_id, consumer_id, consumer_name)
        return {"success": True, "consumer": consumer}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{contract_id}/activate")
async def activate_contract(contract_id: str):
    """Activate a contract"""
    try:
        return contract_manager.activate_contract(contract_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{contract_id}/deprecate")
async def deprecate_contract(contract_id: str, sunset_date: Optional[str] = Query(default=None)):
    """Deprecate a contract"""
    try:
        return contract_manager.deprecate_contract(contract_id, sunset_date)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{contract_id}/sla")
async def set_contract_sla(
    contract_id: str,
    freshness_minutes: Optional[int] = Query(default=None),
    availability_percent: Optional[float] = Query(default=None),
    completeness_percent: Optional[float] = Query(default=None)
):
    """Set SLA for contract"""
    try:
        return contract_manager.set_sla(contract_id, freshness_minutes, availability_percent, completeness_percent)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{contract_id}/sla/check")
async def check_sla(contract_id: str):
    """Check SLA compliance"""
    try:
        return contract_manager.check_sla_compliance(contract_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/notifications")
async def get_notifications(limit: int = Query(default=50)):
    """Get contract notifications"""
    return {"notifications": contract_manager.notifications[-limit:]}


@router.get("/dependencies")
async def get_dependency_graph():
    """Get contract dependency graph"""
    return contract_manager.get_dependency_graph()
