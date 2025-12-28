"""
Integration Hub Routes for ACA DataHub
Third-party integrations: Salesforce, HubSpot, Twilio, Webhooks
"""

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

router = APIRouter(prefix="/integrations", tags=["Integrations"])


# =========================================================================
# Models
# =========================================================================

class IntegrationType(str, Enum):
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    TWILIO = "twilio"
    SENDGRID = "sendgrid"
    MAILCHIMP = "mailchimp"
    GOOGLE_SHEETS = "google_sheets"
    SLACK = "slack"
    ZAPIER = "zapier"


class IntegrationStatus(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    PENDING = "pending"
    ERROR = "error"


class WebhookEvent(str, Enum):
    LEAD_CREATED = "lead.created"
    LEAD_SCORED = "lead.scored"
    POPULATION_CREATED = "population.created"
    CAMPAIGN_SENT = "campaign.sent"
    CAMPAIGN_COMPLETED = "campaign.completed"
    IMPORT_COMPLETED = "import.completed"


# =========================================================================
# Integration Store
# =========================================================================

class IntegrationStore:
    """Manages integration connections and webhooks"""
    
    def __init__(self):
        self.connections: Dict[str, dict] = {}
        self.webhooks: Dict[str, dict] = {}
        self._webhook_counter = 0
        
        # Initialize available integrations catalog
        self.catalog = {
            IntegrationType.SALESFORCE.value: {
                "name": "Salesforce",
                "description": "Sync leads and contacts with Salesforce CRM",
                "icon": "salesforce",
                "category": "CRM",
                "auth_type": "oauth2",
                "features": ["bi-directional sync", "custom objects", "automated update"]
            },
            IntegrationType.HUBSPOT.value: {
                "name": "HubSpot",
                "description": "Connect with HubSpot CRM and marketing",
                "icon": "hubspot",
                "category": "CRM",
                "auth_type": "oauth2",
                "features": ["contact sync", "deal pipeline", "form submissions"]
            },
            IntegrationType.TWILIO.value: {
                "name": "Twilio",
                "description": "Send SMS messages via Twilio",
                "icon": "twilio",
                "category": "Communication",
                "auth_type": "api_key",
                "features": ["SMS sending", "delivery status", "two-way SMS"]
            },
            IntegrationType.SENDGRID.value: {
                "name": "SendGrid",
                "description": "Professional email delivery",
                "icon": "sendgrid",
                "category": "Email",
                "auth_type": "api_key",
                "features": ["email delivery", "templates", "analytics"]
            },
            IntegrationType.MAILCHIMP.value: {
                "name": "Mailchimp",
                "description": "Email marketing automation",
                "icon": "mailchimp",
                "category": "Email",
                "auth_type": "oauth2",
                "features": ["audience sync", "campaign automation", "templates"]
            },
            IntegrationType.GOOGLE_SHEETS.value: {
                "name": "Google Sheets",
                "description": "Bi-directional sync with Google Sheets",
                "icon": "google-sheets",
                "category": "Productivity",
                "auth_type": "oauth2",
                "features": ["import/export", "real-time sync", "scheduled sync"]
            },
            IntegrationType.SLACK.value: {
                "name": "Slack",
                "description": "Team notifications and alerts",
                "icon": "slack",
                "category": "Communication",
                "auth_type": "oauth2",
                "features": ["notifications", "alerts", "reports"]
            },
            IntegrationType.ZAPIER.value: {
                "name": "Zapier",
                "description": "Connect to 5000+ apps via Zapier",
                "icon": "zapier",
                "category": "Automation",
                "auth_type": "webhook",
                "features": ["triggers", "actions", "custom workflows"]
            }
        }
    
    def connect(self, integration_type: str, config: dict) -> dict:
        """Connect an integration"""
        connection_id = f"conn_{integration_type}_{datetime.utcnow().timestamp()}"
        
        connection = {
            "id": connection_id,
            "type": integration_type,
            "status": IntegrationStatus.CONNECTED.value,
            "config": {k: v for k, v in config.items() if k not in ["api_key", "secret"]},
            "connected_at": datetime.utcnow().isoformat(),
            "last_sync": None,
            "sync_count": 0,
            "error": None
        }
        
        self.connections[integration_type] = connection
        return connection
    
    def disconnect(self, integration_type: str) -> bool:
        if integration_type in self.connections:
            del self.connections[integration_type]
            return True
        return False
    
    def get_connection(self, integration_type: str) -> Optional[dict]:
        return self.connections.get(integration_type)
    
    def list_connections(self) -> List[dict]:
        return list(self.connections.values())
    
    def update_sync(self, integration_type: str, success: bool, error: str = None):
        if integration_type in self.connections:
            self.connections[integration_type]["last_sync"] = datetime.utcnow().isoformat()
            self.connections[integration_type]["sync_count"] += 1
            if error:
                self.connections[integration_type]["status"] = IntegrationStatus.ERROR.value
                self.connections[integration_type]["error"] = error
            else:
                self.connections[integration_type]["status"] = IntegrationStatus.CONNECTED.value
                self.connections[integration_type]["error"] = None
    
    # Webhook management
    def create_webhook(self, url: str, events: List[str], secret: str = None) -> dict:
        self._webhook_counter += 1
        webhook_id = f"wh_{self._webhook_counter}"
        
        webhook = {
            "id": webhook_id,
            "url": url,
            "events": events,
            "secret": secret,
            "active": True,
            "created_at": datetime.utcnow().isoformat(),
            "last_triggered": None,
            "trigger_count": 0,
            "last_status": None
        }
        
        self.webhooks[webhook_id] = webhook
        return webhook
    
    def list_webhooks(self) -> List[dict]:
        return list(self.webhooks.values())
    
    def delete_webhook(self, webhook_id: str) -> bool:
        if webhook_id in self.webhooks:
            del self.webhooks[webhook_id]
            return True
        return False


integration_store = IntegrationStore()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("")
async def list_available_integrations():
    """List all available integrations with their status"""
    result = []
    
    for type_key, info in integration_store.catalog.items():
        connection = integration_store.get_connection(type_key)
        result.append({
            **info,
            "type": type_key,
            "status": connection["status"] if connection else IntegrationStatus.DISCONNECTED.value,
            "connected": connection is not None
        })
    
    return {"integrations": result}


@router.get("/connections")
async def list_connected_integrations():
    """List all connected integrations"""
    return {"connections": integration_store.list_connections()}


@router.post("/{integration_type}/connect")
async def connect_integration(
    integration_type: str,
    config: dict
):
    """Connect an integration"""
    if integration_type not in integration_store.catalog:
        raise HTTPException(status_code=400, detail="Unknown integration type")
    
    # In production, would validate credentials and perform OAuth
    connection = integration_store.connect(integration_type, config)
    return {"success": True, "connection": connection}


@router.delete("/{integration_type}")
async def disconnect_integration(integration_type: str):
    """Disconnect an integration"""
    if not integration_store.disconnect(integration_type):
        raise HTTPException(status_code=404, detail="Integration not connected")
    return {"success": True}


@router.post("/{integration_type}/sync")
async def trigger_sync(integration_type: str):
    """Trigger a sync for an integration"""
    connection = integration_store.get_connection(integration_type)
    if not connection:
        raise HTTPException(status_code=404, detail="Integration not connected")
    
    # Simulate sync
    integration_store.update_sync(integration_type, True)
    
    return {"success": True, "message": f"Sync triggered for {integration_type}"}


@router.get("/{integration_type}/status")
async def get_integration_status(integration_type: str):
    """Get status and sync history for an integration"""
    connection = integration_store.get_connection(integration_type)
    if not connection:
        raise HTTPException(status_code=404, detail="Integration not connected")
    
    return connection


# =========================================================================
# Webhooks
# =========================================================================

@router.post("/webhooks")
async def create_webhook(
    url: str,
    events: List[str],
    secret: Optional[str] = None
):
    """Create an outgoing webhook"""
    # Validate events
    valid_events = [e.value for e in WebhookEvent]
    for event in events:
        if event not in valid_events:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid event: {event}. Valid events: {valid_events}"
            )
    
    webhook = integration_store.create_webhook(url, events, secret)
    return {"success": True, "webhook": webhook}


@router.get("/webhooks")
async def list_webhooks():
    """List all configured webhooks"""
    return {"webhooks": integration_store.list_webhooks()}


@router.delete("/webhooks/{webhook_id}")
async def delete_webhook(webhook_id: str):
    """Delete a webhook"""
    if not integration_store.delete_webhook(webhook_id):
        raise HTTPException(status_code=404, detail="Webhook not found")
    return {"success": True}


@router.post("/webhooks/{webhook_id}/test")
async def test_webhook(webhook_id: str):
    """Send a test payload to a webhook"""
    webhooks = integration_store.webhooks
    if webhook_id not in webhooks:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    # In production, would actually send HTTP request
    webhook = webhooks[webhook_id]
    webhook["last_triggered"] = datetime.utcnow().isoformat()
    webhook["trigger_count"] += 1
    webhook["last_status"] = 200
    
    return {
        "success": True,
        "message": "Test payload sent",
        "url": webhook["url"]
    }


# =========================================================================
# CRM Sync Helpers
# =========================================================================

@router.post("/salesforce/push")
async def push_to_salesforce(
    leads: List[dict],
    object_type: str = Query(default="Lead")
):
    """Push leads to Salesforce"""
    connection = integration_store.get_connection("salesforce")
    if not connection:
        raise HTTPException(status_code=404, detail="Salesforce not connected")
    
    # Simulate push
    return {
        "success": True,
        "pushed": len(leads),
        "object_type": object_type,
        "message": f"Pushed {len(leads)} records to Salesforce"
    }


@router.post("/hubspot/push")
async def push_to_hubspot(
    contacts: List[dict]
):
    """Push contacts to HubSpot"""
    connection = integration_store.get_connection("hubspot")
    if not connection:
        raise HTTPException(status_code=404, detail="HubSpot not connected")
    
    return {
        "success": True,
        "pushed": len(contacts),
        "message": f"Pushed {len(contacts)} contacts to HubSpot"
    }


@router.post("/twilio/send")
async def send_sms_via_twilio(
    to: str,
    message: str
):
    """Send SMS via Twilio"""
    connection = integration_store.get_connection("twilio")
    if not connection:
        raise HTTPException(status_code=404, detail="Twilio not connected")
    
    # Simulate send
    return {
        "success": True,
        "to": to,
        "message_sid": f"SM_{''.join([str(hash(to + message))[:20]])}"
    }
