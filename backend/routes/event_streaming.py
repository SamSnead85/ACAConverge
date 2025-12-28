"""
Event Streaming & CDC Routes for ACA DataHub
Kafka integration, change data capture, event sourcing, and stream processing
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json

router = APIRouter(prefix="/events", tags=["Event Streaming"])


# =========================================================================
# Models
# =========================================================================

class EventType(str, Enum):
    LEAD_CREATED = "lead.created"
    LEAD_UPDATED = "lead.updated"
    LEAD_SCORED = "lead.scored"
    POPULATION_CREATED = "population.created"
    CAMPAIGN_SENT = "campaign.sent"
    USER_LOGIN = "user.login"
    QUERY_EXECUTED = "query.executed"
    EXPORT_COMPLETED = "export.completed"


class StreamStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"


# =========================================================================
# Event Store
# =========================================================================

class EventStore:
    """Event sourcing store for audit and replay"""
    
    def __init__(self):
        self.events: List[dict] = []
        self.streams: Dict[str, dict] = {}
        self.subscriptions: Dict[str, dict] = {}
        self.dlq: List[dict] = []  # Dead letter queue
        self.schemas: Dict[str, dict] = {}
        self._counter = 0
        self._init_schemas()
    
    def _init_schemas(self):
        """Initialize event schemas"""
        self.schemas = {
            EventType.LEAD_CREATED.value: {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "string"},
                    "email": {"type": "string"},
                    "source": {"type": "string"}
                },
                "required": ["lead_id"]
            },
            EventType.LEAD_SCORED.value: {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "string"},
                    "old_score": {"type": "number"},
                    "new_score": {"type": "number"}
                },
                "required": ["lead_id", "new_score"]
            },
            EventType.CAMPAIGN_SENT.value: {
                "type": "object",
                "properties": {
                    "campaign_id": {"type": "string"},
                    "recipient_count": {"type": "integer"},
                    "channel": {"type": "string"}
                },
                "required": ["campaign_id"]
            }
        }
    
    def publish(self, event_type: str, data: dict, source: str = "system") -> dict:
        self._counter += 1
        event_id = f"evt_{self._counter}"
        
        event = {
            "id": event_id,
            "type": event_type,
            "source": source,
            "data": data,
            "timestamp": datetime.utcnow().isoformat(),
            "version": 1,
            "correlation_id": data.get("correlation_id"),
            "causation_id": data.get("causation_id")
        }
        
        self.events.append(event)
        
        # Trigger stream processors
        self._process_event(event)
        
        return event
    
    def _process_event(self, event: dict):
        """Process event through active streams"""
        for stream_id, stream in self.streams.items():
            if stream["status"] == StreamStatus.ACTIVE.value:
                # Check if event matches stream filter
                if self._matches_filter(event, stream.get("filter", {})):
                    if stream_id not in stream.get("processed_events", []):
                        stream.setdefault("processed_events", []).append(event["id"])
                        stream["last_processed"] = datetime.utcnow().isoformat()
    
    def _matches_filter(self, event: dict, filter_config: dict) -> bool:
        if not filter_config:
            return True
        
        if "event_types" in filter_config:
            if event["type"] not in filter_config["event_types"]:
                return False
        
        if "source" in filter_config:
            if event["source"] != filter_config["source"]:
                return False
        
        return True
    
    def get_events(
        self,
        event_type: str = None,
        since: str = None,
        limit: int = 100
    ) -> List[dict]:
        events = self.events
        
        if event_type:
            events = [e for e in events if e["type"] == event_type]
        
        if since:
            events = [e for e in events if e["timestamp"] > since]
        
        return sorted(events, key=lambda x: x["timestamp"], reverse=True)[:limit]
    
    def replay_events(
        self,
        from_timestamp: str = None,
        to_timestamp: str = None,
        event_types: List[str] = None
    ) -> List[dict]:
        """Replay events for rebuilding state"""
        events = self.events
        
        if from_timestamp:
            events = [e for e in events if e["timestamp"] >= from_timestamp]
        
        if to_timestamp:
            events = [e for e in events if e["timestamp"] <= to_timestamp]
        
        if event_types:
            events = [e for e in events if e["type"] in event_types]
        
        return sorted(events, key=lambda x: x["timestamp"])
    
    def create_stream(
        self,
        name: str,
        filter_config: dict = None,
        processor: str = None
    ) -> dict:
        self._counter += 1
        stream_id = f"stream_{self._counter}"
        
        stream = {
            "id": stream_id,
            "name": name,
            "filter": filter_config or {},
            "processor": processor,
            "status": StreamStatus.ACTIVE.value,
            "created_at": datetime.utcnow().isoformat(),
            "last_processed": None,
            "processed_count": 0,
            "error_count": 0
        }
        
        self.streams[stream_id] = stream
        return stream
    
    def get_stream(self, stream_id: str) -> Optional[dict]:
        return self.streams.get(stream_id)
    
    def pause_stream(self, stream_id: str) -> bool:
        if stream_id in self.streams:
            self.streams[stream_id]["status"] = StreamStatus.PAUSED.value
            return True
        return False
    
    def resume_stream(self, stream_id: str) -> bool:
        if stream_id in self.streams:
            self.streams[stream_id]["status"] = StreamStatus.ACTIVE.value
            return True
        return False
    
    def add_to_dlq(self, event: dict, error: str):
        """Add failed event to dead letter queue"""
        self.dlq.append({
            "event": event,
            "error": error,
            "failed_at": datetime.utcnow().isoformat(),
            "retry_count": 0
        })
    
    def get_dlq(self) -> List[dict]:
        return self.dlq
    
    def retry_dlq_event(self, index: int) -> bool:
        if 0 <= index < len(self.dlq):
            item = self.dlq[index]
            item["retry_count"] += 1
            # Attempt reprocessing
            self._process_event(item["event"])
            self.dlq.pop(index)
            return True
        return False


event_store = EventStore()


# =========================================================================
# Change Data Capture
# =========================================================================

class CDCTracker:
    """Track changes for CDC"""
    
    def __init__(self):
        self.changes: List[dict] = []
        self.tracked_tables = ["leads", "populations", "campaigns", "users"]
        self._counter = 0
    
    def capture_change(
        self,
        table: str,
        operation: str,  # INSERT, UPDATE, DELETE
        old_data: dict = None,
        new_data: dict = None,
        primary_key: str = None
    ) -> dict:
        self._counter += 1
        change_id = f"cdc_{self._counter}"
        
        change = {
            "id": change_id,
            "table": table,
            "operation": operation,
            "old_data": old_data,
            "new_data": new_data,
            "primary_key": primary_key,
            "timestamp": datetime.utcnow().isoformat(),
            "lsn": self._counter  # Log sequence number
        }
        
        self.changes.append(change)
        
        # Publish as event
        event_store.publish(
            f"{table}.{operation.lower()}",
            {
                "table": table,
                "operation": operation,
                "record_id": primary_key
            },
            source="cdc"
        )
        
        return change
    
    def get_changes(
        self,
        table: str = None,
        since_lsn: int = 0,
        limit: int = 100
    ) -> List[dict]:
        changes = [c for c in self.changes if c["lsn"] > since_lsn]
        
        if table:
            changes = [c for c in changes if c["table"] == table]
        
        return changes[:limit]


cdc_tracker = CDCTracker()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/publish")
async def publish_event(
    event_type: EventType = Query(...),
    source: str = Query(default="api"),
    data: dict = None
):
    """Publish an event"""
    event = event_store.publish(event_type.value, data or {}, source)
    return {"success": True, "event": event}


@router.get("")
async def list_events(
    event_type: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=1000)
):
    """List events"""
    return {"events": event_store.get_events(event_type, since, limit)}


@router.get("/{event_id}")
async def get_event(event_id: str):
    """Get event by ID"""
    event = next((e for e in event_store.events if e["id"] == event_id), None)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@router.post("/replay")
async def replay_events(
    from_timestamp: Optional[str] = Query(default=None),
    to_timestamp: Optional[str] = Query(default=None),
    event_types: Optional[List[str]] = Query(default=None)
):
    """Replay events for state reconstruction"""
    events = event_store.replay_events(from_timestamp, to_timestamp, event_types)
    return {"events": events, "count": len(events)}


@router.post("/streams")
async def create_stream(
    name: str = Query(...),
    event_types: Optional[List[str]] = Query(default=None),
    source: Optional[str] = Query(default=None)
):
    """Create an event stream"""
    filter_config = {}
    if event_types:
        filter_config["event_types"] = event_types
    if source:
        filter_config["source"] = source
    
    stream = event_store.create_stream(name, filter_config)
    return {"success": True, "stream": stream}


@router.get("/streams")
async def list_streams():
    """List all streams"""
    return {"streams": list(event_store.streams.values())}


@router.get("/streams/{stream_id}")
async def get_stream(stream_id: str):
    """Get stream details"""
    stream = event_store.get_stream(stream_id)
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")
    return stream


@router.post("/streams/{stream_id}/pause")
async def pause_stream(stream_id: str):
    """Pause a stream"""
    if not event_store.pause_stream(stream_id):
        raise HTTPException(status_code=404, detail="Stream not found")
    return {"success": True}


@router.post("/streams/{stream_id}/resume")
async def resume_stream(stream_id: str):
    """Resume a stream"""
    if not event_store.resume_stream(stream_id):
        raise HTTPException(status_code=404, detail="Stream not found")
    return {"success": True}


@router.get("/dlq")
async def get_dead_letter_queue():
    """Get dead letter queue"""
    return {"dlq": event_store.get_dlq()}


@router.post("/dlq/{index}/retry")
async def retry_dlq_event(index: int):
    """Retry a failed event"""
    if not event_store.retry_dlq_event(index):
        raise HTTPException(status_code=404, detail="DLQ item not found")
    return {"success": True}


@router.get("/schemas")
async def list_event_schemas():
    """List event schemas"""
    return {"schemas": event_store.schemas}


@router.get("/schemas/{event_type}")
async def get_event_schema(event_type: str):
    """Get schema for event type"""
    schema = event_store.schemas.get(event_type)
    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    return schema


# CDC Endpoints
@router.post("/cdc/capture")
async def capture_change(
    table: str = Query(...),
    operation: str = Query(...),
    primary_key: str = Query(...),
    old_data: dict = None,
    new_data: dict = None
):
    """Capture a data change"""
    change = cdc_tracker.capture_change(table, operation, old_data, new_data, primary_key)
    return {"success": True, "change": change}


@router.get("/cdc/changes")
async def get_changes(
    table: Optional[str] = Query(default=None),
    since_lsn: int = Query(default=0),
    limit: int = Query(default=100)
):
    """Get CDC changes"""
    return {"changes": cdc_tracker.get_changes(table, since_lsn, limit)}


@router.get("/cdc/tables")
async def get_tracked_tables():
    """Get list of tracked tables"""
    return {"tables": cdc_tracker.tracked_tables}
