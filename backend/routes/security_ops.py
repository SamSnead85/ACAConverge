"""
Security Operations Routes for ACA DataHub
SIEM integration, threat detection, vulnerability scanning, and encryption management
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import hashlib

router = APIRouter(prefix="/security", tags=["Security Operations"])


# =========================================================================
# Models
# =========================================================================

class ThreatLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class EventCategory(str, Enum):
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_ACCESS = "data_access"
    CONFIGURATION = "configuration"
    NETWORK = "network"
    SYSTEM = "system"


# =========================================================================
# Security Event Logger
# =========================================================================

class SecurityEventLogger:
    """Log and analyze security events"""
    
    def __init__(self):
        self.events: List[dict] = []
        self._counter = 0
    
    def log_event(
        self,
        category: str,
        action: str,
        user_id: str = None,
        resource: str = None,
        result: str = "success",
        metadata: dict = None
    ) -> dict:
        self._counter += 1
        event_id = f"sec_evt_{self._counter}"
        
        event = {
            "id": event_id,
            "category": category,
            "action": action,
            "user_id": user_id,
            "resource": resource,
            "result": result,
            "ip_address": f"192.168.1.{random.randint(1, 255)}",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "metadata": metadata or {},
            "timestamp": datetime.utcnow().isoformat(),
            "threat_indicators": []
        }
        
        # Check for threat indicators
        event["threat_indicators"] = self._check_threat_indicators(event)
        
        self.events.append(event)
        return event
    
    def _check_threat_indicators(self, event: dict) -> List[dict]:
        indicators = []
        
        if event["result"] == "failure":
            indicators.append({
                "type": "failed_action",
                "severity": "low",
                "message": f"Failed {event['action']} attempt"
            })
        
        if event["action"] in ["delete", "export", "bulk_update"]:
            indicators.append({
                "type": "sensitive_operation",
                "severity": "medium",
                "message": f"Sensitive operation: {event['action']}"
            })
        
        return indicators
    
    def get_events(
        self,
        category: str = None,
        user_id: str = None,
        since_hours: int = 24,
        limit: int = 100
    ) -> List[dict]:
        cutoff = (datetime.utcnow() - timedelta(hours=since_hours)).isoformat()
        events = [e for e in self.events if e["timestamp"] > cutoff]
        
        if category:
            events = [e for e in events if e["category"] == category]
        
        if user_id:
            events = [e for e in events if e["user_id"] == user_id]
        
        return events[-limit:]
    
    def detect_anomalies(self, user_id: str) -> dict:
        """Detect access anomalies for user"""
        user_events = [e for e in self.events if e.get("user_id") == user_id]
        
        anomalies = []
        
        # Check for unusual volume
        if len(user_events) > 100:
            anomalies.append({
                "type": "high_volume",
                "severity": "medium",
                "description": f"User has {len(user_events)} events (high volume)"
            })
        
        # Check for failed attempts
        failures = sum(1 for e in user_events if e.get("result") == "failure")
        if failures > 5:
            anomalies.append({
                "type": "multiple_failures",
                "severity": "high",
                "description": f"{failures} failed attempts detected"
            })
        
        # Check for sensitive operations
        sensitive = sum(1 for e in user_events if e.get("action") in ["delete", "export"])
        if sensitive > 3:
            anomalies.append({
                "type": "sensitive_activity",
                "severity": "medium",
                "description": f"{sensitive} sensitive operations performed"
            })
        
        return {
            "user_id": user_id,
            "total_events": len(user_events),
            "anomalies": anomalies,
            "risk_score": min(100, len(anomalies) * 25 + failures * 5)
        }


security_logger = SecurityEventLogger()


# =========================================================================
# Threat Detector
# =========================================================================

class ThreatDetector:
    """Detect and analyze security threats"""
    
    def __init__(self):
        self.threats: List[dict] = []
        self.rules: Dict[str, dict] = {}
        self._counter = 0
        self._init_rules()
    
    def _init_rules(self):
        self.rules = {
            "brute_force": {
                "name": "Brute Force Detection",
                "condition": {"event_type": "login_failure", "count": 5, "window_minutes": 10},
                "severity": "high"
            },
            "data_exfiltration": {
                "name": "Data Exfiltration Attempt",
                "condition": {"event_type": "export", "size_threshold_mb": 100},
                "severity": "critical"
            },
            "privilege_escalation": {
                "name": "Privilege Escalation",
                "condition": {"event_type": "role_change", "to_role": "admin"},
                "severity": "high"
            },
            "unusual_access_time": {
                "name": "Unusual Access Time",
                "condition": {"hour_range": [0, 5]},
                "severity": "medium"
            }
        }
    
    def detect_threats(self, events: List[dict]) -> List[dict]:
        """Detect threats from events"""
        detected = []
        
        for rule_id, rule in self.rules.items():
            if self._check_rule(rule, events):
                self._counter += 1
                threat = {
                    "id": f"threat_{self._counter}",
                    "rule_id": rule_id,
                    "rule_name": rule["name"],
                    "severity": rule["severity"],
                    "detected_at": datetime.utcnow().isoformat(),
                    "status": "open",
                    "evidence": events[:5]
                }
                detected.append(threat)
                self.threats.append(threat)
        
        return detected
    
    def _check_rule(self, rule: dict, events: List[dict]) -> bool:
        # Simulate rule matching
        return random.random() > 0.9
    
    def get_active_threats(self) -> List[dict]:
        return [t for t in self.threats if t["status"] == "open"]


threat_detector = ThreatDetector()


# =========================================================================
# Vulnerability Scanner
# =========================================================================

class VulnerabilityScanner:
    """Scan for security vulnerabilities"""
    
    def __init__(self):
        self.scans: List[dict] = []
        self._counter = 0
    
    def run_scan(self, target: str, scan_type: str = "full") -> dict:
        self._counter += 1
        scan_id = f"scan_{self._counter}"
        
        # Simulate vulnerability findings
        vulnerabilities = []
        
        vuln_types = [
            ("SQL Injection", "high", "Input validation"),
            ("XSS", "medium", "Output encoding"),
            ("Insecure Configuration", "low", "Configuration"),
            ("Outdated Dependency", "medium", "Dependency"),
            ("Weak Encryption", "high", "Cryptography")
        ]
        
        num_vulns = random.randint(0, 5)
        for i in range(num_vulns):
            vuln_type = random.choice(vuln_types)
            vulnerabilities.append({
                "id": f"vuln_{scan_id}_{i+1}",
                "type": vuln_type[0],
                "severity": vuln_type[1],
                "category": vuln_type[2],
                "location": f"/api/endpoint/{random.randint(1, 10)}",
                "description": f"Potential {vuln_type[0]} vulnerability detected",
                "remediation": f"Apply appropriate {vuln_type[2].lower()} fixes"
            })
        
        scan = {
            "id": scan_id,
            "target": target,
            "scan_type": scan_type,
            "status": "completed",
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": datetime.utcnow().isoformat(),
            "vulnerabilities": vulnerabilities,
            "summary": {
                "total": len(vulnerabilities),
                "critical": sum(1 for v in vulnerabilities if v["severity"] == "critical"),
                "high": sum(1 for v in vulnerabilities if v["severity"] == "high"),
                "medium": sum(1 for v in vulnerabilities if v["severity"] == "medium"),
                "low": sum(1 for v in vulnerabilities if v["severity"] == "low")
            }
        }
        
        self.scans.append(scan)
        return scan


vuln_scanner = VulnerabilityScanner()


# =========================================================================
# Encryption Manager
# =========================================================================

class EncryptionManager:
    """Manage encryption keys and operations"""
    
    def __init__(self):
        self.keys: Dict[str, dict] = {}
        self._counter = 0
        self._init_keys()
    
    def _init_keys(self):
        self.keys = {
            "master_key": {
                "id": "master_key",
                "name": "Master Encryption Key",
                "algorithm": "AES-256-GCM",
                "created_at": (datetime.utcnow() - timedelta(days=180)).isoformat(),
                "expires_at": (datetime.utcnow() + timedelta(days=185)).isoformat(),
                "status": "active",
                "usage_count": random.randint(10000, 100000)
            },
            "data_key": {
                "id": "data_key",
                "name": "Data Encryption Key",
                "algorithm": "AES-256-GCM",
                "created_at": (datetime.utcnow() - timedelta(days=90)).isoformat(),
                "expires_at": (datetime.utcnow() + timedelta(days=275)).isoformat(),
                "status": "active",
                "usage_count": random.randint(50000, 500000)
            }
        }
    
    def rotate_key(self, key_id: str) -> dict:
        if key_id not in self.keys:
            raise ValueError("Key not found")
        
        old_key = self.keys[key_id]
        old_key["status"] = "rotating"
        
        self._counter += 1
        new_key_id = f"{key_id}_v{self._counter}"
        
        new_key = {
            "id": new_key_id,
            "name": old_key["name"],
            "algorithm": old_key["algorithm"],
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(days=365)).isoformat(),
            "status": "active",
            "usage_count": 0,
            "previous_key": key_id
        }
        
        self.keys[new_key_id] = new_key
        old_key["status"] = "retired"
        old_key["retired_at"] = datetime.utcnow().isoformat()
        
        return {
            "new_key": new_key,
            "old_key": old_key,
            "rotation_completed": datetime.utcnow().isoformat()
        }
    
    def get_key_status(self) -> dict:
        active = sum(1 for k in self.keys.values() if k["status"] == "active")
        expiring_soon = sum(1 for k in self.keys.values() 
                          if k["status"] == "active" and 
                          k["expires_at"] < (datetime.utcnow() + timedelta(days=30)).isoformat())
        
        return {
            "total_keys": len(self.keys),
            "active_keys": active,
            "expiring_soon": expiring_soon,
            "keys": list(self.keys.values())
        }


encryption_manager = EncryptionManager()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/events")
async def log_security_event(
    category: EventCategory = Query(...),
    action: str = Query(...),
    user_id: Optional[str] = Query(default=None),
    resource: Optional[str] = Query(default=None),
    result: str = Query(default="success")
):
    """Log a security event"""
    event = security_logger.log_event(category.value, action, user_id, resource, result)
    return {"success": True, "event": event}


@router.get("/events")
async def get_security_events(
    category: Optional[str] = Query(default=None),
    user_id: Optional[str] = Query(default=None),
    since_hours: int = Query(default=24),
    limit: int = Query(default=100)
):
    """Get security events"""
    return {"events": security_logger.get_events(category, user_id, since_hours, limit)}


@router.get("/anomalies/{user_id}")
async def detect_user_anomalies(user_id: str):
    """Detect access anomalies for user"""
    return security_logger.detect_anomalies(user_id)


@router.post("/threats/detect")
async def detect_threats(events: List[dict] = None):
    """Detect threats from events"""
    events = events or security_logger.events[-100:]
    detected = threat_detector.detect_threats(events)
    return {"threats": detected}


@router.get("/threats")
async def get_active_threats():
    """Get active threats"""
    return {"threats": threat_detector.get_active_threats()}


@router.get("/threats/rules")
async def get_threat_rules():
    """Get threat detection rules"""
    return {"rules": threat_detector.rules}


@router.post("/scan")
async def run_vulnerability_scan(
    target: str = Query(...),
    scan_type: str = Query(default="full")
):
    """Run vulnerability scan"""
    return vuln_scanner.run_scan(target, scan_type)


@router.get("/scans")
async def get_scan_history():
    """Get scan history"""
    return {"scans": vuln_scanner.scans[-20:]}


@router.get("/encryption/keys")
async def get_encryption_keys():
    """Get encryption key status"""
    return encryption_manager.get_key_status()


@router.post("/encryption/keys/{key_id}/rotate")
async def rotate_encryption_key(key_id: str):
    """Rotate encryption key"""
    try:
        return encryption_manager.rotate_key(key_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/dashboard")
async def get_security_dashboard():
    """Get security operations dashboard"""
    events = security_logger.events[-1000:]
    
    return {
        "summary": {
            "total_events_24h": len(security_logger.get_events(since_hours=24)),
            "active_threats": len(threat_detector.get_active_threats()),
            "failed_auth_attempts": sum(1 for e in events 
                                        if e.get("category") == "authentication" 
                                        and e.get("result") == "failure"),
            "sensitive_operations": sum(1 for e in events 
                                        if e.get("action") in ["delete", "export", "bulk_update"])
        },
        "encryption": encryption_manager.get_key_status(),
        "recent_scans": vuln_scanner.scans[-5:]
    }
