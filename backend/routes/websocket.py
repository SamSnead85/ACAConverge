"""
WebSocket Routes for ACA DataHub
Real-time collaboration infrastructure with presence, notifications, and live updates
"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Dict, Set, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json
import asyncio

router = APIRouter(tags=["WebSocket"])


@dataclass
class Connection:
    """Represents an active WebSocket connection"""
    websocket: WebSocket
    user_id: str
    user_name: str
    workspace_id: Optional[str] = None
    connected_at: datetime = field(default_factory=datetime.utcnow)


class ConnectionManager:
    """Manages WebSocket connections for real-time features"""
    
    def __init__(self):
        # workspace_id -> set of connections
        self.workspaces: Dict[str, Set[Connection]] = {}
        # user_id -> connection (for direct notifications)
        self.users: Dict[str, Connection] = {}
        # user_id -> set of workspace_ids (tracking presence)
        self.presence: Dict[str, Set[str]] = {}
    
    async def connect(
        self,
        websocket: WebSocket,
        user_id: str,
        user_name: str,
        workspace_id: Optional[str] = None
    ) -> Connection:
        """Accept a new WebSocket connection"""
        await websocket.accept()
        
        connection = Connection(
            websocket=websocket,
            user_id=user_id,
            user_name=user_name,
            workspace_id=workspace_id
        )
        
        # Track user connection
        self.users[user_id] = connection
        
        # Track workspace presence
        if workspace_id:
            if workspace_id not in self.workspaces:
                self.workspaces[workspace_id] = set()
            self.workspaces[workspace_id].add(connection)
            
            if user_id not in self.presence:
                self.presence[user_id] = set()
            self.presence[user_id].add(workspace_id)
            
            # Notify others in workspace
            await self.broadcast_to_workspace(
                workspace_id,
                {
                    "type": "user_joined",
                    "user_id": user_id,
                    "user_name": user_name,
                    "timestamp": datetime.utcnow().isoformat()
                },
                exclude_user=user_id
            )
        
        return connection
    
    async def disconnect(self, connection: Connection):
        """Handle WebSocket disconnection"""
        user_id = connection.user_id
        workspace_id = connection.workspace_id
        
        # Remove from user tracking
        if user_id in self.users:
            del self.users[user_id]
        
        # Remove from workspace
        if workspace_id and workspace_id in self.workspaces:
            self.workspaces[workspace_id].discard(connection)
            
            if user_id in self.presence:
                self.presence[user_id].discard(workspace_id)
            
            # Notify others
            await self.broadcast_to_workspace(
                workspace_id,
                {
                    "type": "user_left",
                    "user_id": user_id,
                    "user_name": connection.user_name,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    async def broadcast_to_workspace(
        self,
        workspace_id: str,
        message: dict,
        exclude_user: Optional[str] = None
    ):
        """Send message to all connections in a workspace"""
        if workspace_id not in self.workspaces:
            return
        
        message_str = json.dumps(message)
        dead_connections = []
        
        for conn in self.workspaces[workspace_id]:
            if exclude_user and conn.user_id == exclude_user:
                continue
            try:
                await conn.websocket.send_text(message_str)
            except Exception:
                dead_connections.append(conn)
        
        # Clean up dead connections
        for conn in dead_connections:
            self.workspaces[workspace_id].discard(conn)
    
    async def send_to_user(self, user_id: str, message: dict):
        """Send a direct message to a specific user"""
        if user_id not in self.users:
            return False
        
        try:
            await self.users[user_id].websocket.send_text(json.dumps(message))
            return True
        except Exception:
            return False
    
    def get_workspace_users(self, workspace_id: str) -> List[dict]:
        """Get list of users currently in a workspace"""
        if workspace_id not in self.workspaces:
            return []
        
        return [
            {
                "user_id": conn.user_id,
                "user_name": conn.user_name,
                "connected_at": conn.connected_at.isoformat()
            }
            for conn in self.workspaces[workspace_id]
        ]
    
    def get_user_workspaces(self, user_id: str) -> List[str]:
        """Get list of workspaces a user is in"""
        return list(self.presence.get(user_id, set()))


# Global connection manager
manager = ConnectionManager()


# =========================================================================
# Notification Store (In-memory for demo, use Redis in production)
# =========================================================================

class NotificationStore:
    """Stores notifications for users"""
    
    def __init__(self):
        # user_id -> list of notifications
        self.notifications: Dict[str, List[dict]] = {}
        self.max_notifications = 100
    
    def add(self, user_id: str, notification: dict):
        """Add a notification for a user"""
        if user_id not in self.notifications:
            self.notifications[user_id] = []
        
        notification["id"] = f"notif_{len(self.notifications[user_id])}_{datetime.utcnow().timestamp()}"
        notification["created_at"] = datetime.utcnow().isoformat()
        notification["read"] = False
        
        self.notifications[user_id].insert(0, notification)
        
        # Trim old notifications
        if len(self.notifications[user_id]) > self.max_notifications:
            self.notifications[user_id] = self.notifications[user_id][:self.max_notifications]
        
        return notification
    
    def get(self, user_id: str, unread_only: bool = False) -> List[dict]:
        """Get notifications for a user"""
        if user_id not in self.notifications:
            return []
        
        if unread_only:
            return [n for n in self.notifications[user_id] if not n.get("read")]
        
        return self.notifications[user_id]
    
    def mark_read(self, user_id: str, notification_id: str):
        """Mark a notification as read"""
        if user_id not in self.notifications:
            return
        
        for n in self.notifications[user_id]:
            if n["id"] == notification_id:
                n["read"] = True
                break
    
    def mark_all_read(self, user_id: str):
        """Mark all notifications as read"""
        if user_id not in self.notifications:
            return
        
        for n in self.notifications[user_id]:
            n["read"] = True
    
    def get_unread_count(self, user_id: str) -> int:
        """Get count of unread notifications"""
        if user_id not in self.notifications:
            return 0
        return sum(1 for n in self.notifications[user_id] if not n.get("read"))


notification_store = NotificationStore()


# =========================================================================
# WebSocket Endpoints
# =========================================================================

@router.websocket("/ws/workspace/{workspace_id}")
async def workspace_websocket(
    websocket: WebSocket,
    workspace_id: str,
    user_id: str = Query(...),
    user_name: str = Query(default="Anonymous")
):
    """
    WebSocket endpoint for workspace collaboration.
    
    Handles:
    - Presence (who's online)
    - Real-time query updates
    - Cursor positions
    - Comments/annotations
    """
    connection = await manager.connect(websocket, user_id, user_name, workspace_id)
    
    try:
        # Send current workspace state
        await websocket.send_json({
            "type": "workspace_state",
            "users": manager.get_workspace_users(workspace_id),
            "workspace_id": workspace_id
        })
        
        # Handle incoming messages
        while True:
            data = await websocket.receive_json()
            message_type = data.get("type")
            
            if message_type == "cursor_move":
                # Broadcast cursor position
                await manager.broadcast_to_workspace(
                    workspace_id,
                    {
                        "type": "cursor_update",
                        "user_id": user_id,
                        "user_name": user_name,
                        "position": data.get("position"),
                        "timestamp": datetime.utcnow().isoformat()
                    },
                    exclude_user=user_id
                )
            
            elif message_type == "typing":
                # Broadcast typing indicator
                await manager.broadcast_to_workspace(
                    workspace_id,
                    {
                        "type": "user_typing",
                        "user_id": user_id,
                        "user_name": user_name,
                        "field": data.get("field"),
                        "timestamp": datetime.utcnow().isoformat()
                    },
                    exclude_user=user_id
                )
            
            elif message_type == "query_update":
                # Broadcast query change
                await manager.broadcast_to_workspace(
                    workspace_id,
                    {
                        "type": "query_changed",
                        "user_id": user_id,
                        "user_name": user_name,
                        "query": data.get("query"),
                        "timestamp": datetime.utcnow().isoformat()
                    },
                    exclude_user=user_id
                )
            
            elif message_type == "comment":
                # Handle comment
                comment = {
                    "type": "new_comment",
                    "user_id": user_id,
                    "user_name": user_name,
                    "content": data.get("content"),
                    "target": data.get("target"),  # row, column, query
                    "target_id": data.get("target_id"),
                    "mentions": data.get("mentions", []),
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                await manager.broadcast_to_workspace(workspace_id, comment)
                
                # Send notifications to mentioned users
                for mentioned_user in data.get("mentions", []):
                    notification = notification_store.add(mentioned_user, {
                        "type": "mention",
                        "title": f"{user_name} mentioned you",
                        "body": data.get("content", "")[:100],
                        "workspace_id": workspace_id
                    })
                    await manager.send_to_user(mentioned_user, {
                        "type": "notification",
                        "notification": notification
                    })
            
            elif message_type == "ping":
                # Heartbeat
                await websocket.send_json({"type": "pong"})
    
    except WebSocketDisconnect:
        await manager.disconnect(connection)


@router.websocket("/ws/notifications/{user_id}")
async def notifications_websocket(
    websocket: WebSocket,
    user_id: str
):
    """
    WebSocket endpoint for user notifications.
    
    Handles:
    - Real-time notification delivery
    - Unread count updates
    """
    await websocket.accept()
    
    # Store connection for notifications
    manager.users[user_id] = Connection(
        websocket=websocket,
        user_id=user_id,
        user_name=""
    )
    
    try:
        # Send initial notification state
        await websocket.send_json({
            "type": "notifications_state",
            "notifications": notification_store.get(user_id)[:20],
            "unread_count": notification_store.get_unread_count(user_id)
        })
        
        # Keep connection alive
        while True:
            data = await websocket.receive_json()
            
            if data.get("type") == "mark_read":
                notification_id = data.get("notification_id")
                if notification_id:
                    notification_store.mark_read(user_id, notification_id)
                else:
                    notification_store.mark_all_read(user_id)
                
                await websocket.send_json({
                    "type": "unread_count",
                    "count": notification_store.get_unread_count(user_id)
                })
            
            elif data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
    
    except WebSocketDisconnect:
        if user_id in manager.users:
            del manager.users[user_id]


# =========================================================================
# REST Endpoints for Notifications
# =========================================================================

@router.get("/api/notifications")
async def get_notifications(
    user_id: str = Query(...),
    unread_only: bool = Query(default=False)
):
    """Get notifications for a user"""
    return {
        "notifications": notification_store.get(user_id, unread_only),
        "unread_count": notification_store.get_unread_count(user_id)
    }


@router.post("/api/notifications/mark-read")
async def mark_notifications_read(
    user_id: str = Query(...),
    notification_id: Optional[str] = Query(default=None)
):
    """Mark notifications as read"""
    if notification_id:
        notification_store.mark_read(user_id, notification_id)
    else:
        notification_store.mark_all_read(user_id)
    
    return {"success": True, "unread_count": notification_store.get_unread_count(user_id)}


@router.get("/api/workspace/{workspace_id}/presence")
async def get_workspace_presence(workspace_id: str):
    """Get list of users currently in a workspace"""
    return {
        "workspace_id": workspace_id,
        "users": manager.get_workspace_users(workspace_id)
    }


# =========================================================================
# Activity Feed
# =========================================================================

class ActivityFeed:
    """Stores workspace activity for activity feed"""
    
    def __init__(self):
        # workspace_id -> list of activities
        self.activities: Dict[str, List[dict]] = {}
        self.max_activities = 500
    
    def add(self, workspace_id: str, activity: dict):
        """Add an activity to the feed"""
        if workspace_id not in self.activities:
            self.activities[workspace_id] = []
        
        activity["id"] = f"act_{len(self.activities[workspace_id])}_{datetime.utcnow().timestamp()}"
        activity["timestamp"] = datetime.utcnow().isoformat()
        
        self.activities[workspace_id].insert(0, activity)
        
        if len(self.activities[workspace_id]) > self.max_activities:
            self.activities[workspace_id] = self.activities[workspace_id][:self.max_activities]
    
    def get(self, workspace_id: str, limit: int = 50) -> List[dict]:
        """Get recent activities for a workspace"""
        if workspace_id not in self.activities:
            return []
        return self.activities[workspace_id][:limit]


activity_feed = ActivityFeed()


@router.get("/api/workspace/{workspace_id}/activity")
async def get_workspace_activity(
    workspace_id: str,
    limit: int = Query(default=50, le=100)
):
    """Get activity feed for a workspace"""
    return {
        "workspace_id": workspace_id,
        "activities": activity_feed.get(workspace_id, limit)
    }


# Helper function to log activity (call from other routes)
async def log_activity(
    workspace_id: str,
    user_id: str,
    user_name: str,
    action: str,
    details: dict = None
):
    """Log an activity and broadcast to workspace"""
    activity = {
        "user_id": user_id,
        "user_name": user_name,
        "action": action,
        "details": details or {}
    }
    
    activity_feed.add(workspace_id, activity)
    
    await manager.broadcast_to_workspace(
        workspace_id,
        {
            "type": "new_activity",
            "activity": activity
        }
    )
