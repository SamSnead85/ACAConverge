"""
Collaboration Hub Routes for ACA DataHub
Team workspaces, shared queries, annotations, and activity feeds
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import random

router = APIRouter(prefix="/collaboration", tags=["Collaboration Hub"])


# =========================================================================
# Models
# =========================================================================

class AssetType(str, Enum):
    QUERY = "query"
    DASHBOARD = "dashboard"
    REPORT = "report"
    POPULATION = "population"


class PermissionLevel(str, Enum):
    VIEW = "view"
    EDIT = "edit"
    ADMIN = "admin"


# =========================================================================
# Workspace Manager
# =========================================================================

class WorkspaceManager:
    """Manage team workspaces"""
    
    def __init__(self):
        self.workspaces: Dict[str, dict] = {}
        self.members: Dict[str, List[dict]] = {}
        self._counter = 0
    
    def create_workspace(
        self,
        name: str,
        description: str,
        owner_id: str
    ) -> dict:
        self._counter += 1
        workspace_id = f"ws_{self._counter}"
        
        workspace = {
            "id": workspace_id,
            "name": name,
            "description": description,
            "owner_id": owner_id,
            "created_at": datetime.utcnow().isoformat(),
            "member_count": 1,
            "asset_count": 0
        }
        
        self.workspaces[workspace_id] = workspace
        self.members[workspace_id] = [{
            "user_id": owner_id,
            "role": "owner",
            "joined_at": datetime.utcnow().isoformat()
        }]
        
        return workspace
    
    def add_member(
        self,
        workspace_id: str,
        user_id: str,
        role: str = "member"
    ) -> dict:
        if workspace_id not in self.members:
            raise ValueError("Workspace not found")
        
        member = {
            "user_id": user_id,
            "role": role,
            "joined_at": datetime.utcnow().isoformat()
        }
        
        self.members[workspace_id].append(member)
        self.workspaces[workspace_id]["member_count"] += 1
        
        return member
    
    def get_workspace(self, workspace_id: str) -> dict:
        ws = self.workspaces.get(workspace_id)
        if not ws:
            raise ValueError("Workspace not found")
        
        return {
            **ws,
            "members": self.members.get(workspace_id, [])
        }


workspaces = WorkspaceManager()


# =========================================================================
# Shared Assets Manager
# =========================================================================

class SharedAssetsManager:
    """Manage shared queries, reports, and dashboards"""
    
    def __init__(self):
        self.assets: Dict[str, dict] = {}
        self.shares: Dict[str, List[dict]] = {}
        self.versions: Dict[str, List[dict]] = {}
        self._counter = 0
    
    def create_asset(
        self,
        workspace_id: str,
        asset_type: str,
        name: str,
        content: Any,
        created_by: str
    ) -> dict:
        self._counter += 1
        asset_id = f"asset_{self._counter}"
        
        asset = {
            "id": asset_id,
            "workspace_id": workspace_id,
            "type": asset_type,
            "name": name,
            "content": content,
            "created_by": created_by,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "version": 1
        }
        
        self.assets[asset_id] = asset
        self.versions[asset_id] = [{
            "version": 1,
            "content": content,
            "created_by": created_by,
            "created_at": datetime.utcnow().isoformat()
        }]
        
        return asset
    
    def update_asset(
        self,
        asset_id: str,
        content: Any,
        updated_by: str
    ) -> dict:
        if asset_id not in self.assets:
            raise ValueError("Asset not found")
        
        asset = self.assets[asset_id]
        asset["version"] += 1
        asset["content"] = content
        asset["updated_at"] = datetime.utcnow().isoformat()
        
        self.versions[asset_id].append({
            "version": asset["version"],
            "content": content,
            "created_by": updated_by,
            "created_at": datetime.utcnow().isoformat()
        })
        
        return asset
    
    def share_asset(
        self,
        asset_id: str,
        user_id: str,
        permission: str
    ) -> dict:
        if asset_id not in self.assets:
            raise ValueError("Asset not found")
        
        share = {
            "user_id": user_id,
            "permission": permission,
            "shared_at": datetime.utcnow().isoformat()
        }
        
        if asset_id not in self.shares:
            self.shares[asset_id] = []
        
        self.shares[asset_id].append(share)
        return share
    
    def get_versions(self, asset_id: str) -> List[dict]:
        return self.versions.get(asset_id, [])


assets = SharedAssetsManager()


# =========================================================================
# Comments & Annotations
# =========================================================================

class CommentManager:
    """Manage comments and annotations"""
    
    def __init__(self):
        self.comments: Dict[str, List[dict]] = {}
        self._counter = 0
    
    def add_comment(
        self,
        asset_id: str,
        user_id: str,
        content: str,
        parent_id: str = None
    ) -> dict:
        self._counter += 1
        comment_id = f"comment_{self._counter}"
        
        comment = {
            "id": comment_id,
            "asset_id": asset_id,
            "user_id": user_id,
            "content": content,
            "parent_id": parent_id,
            "created_at": datetime.utcnow().isoformat(),
            "resolved": False
        }
        
        if asset_id not in self.comments:
            self.comments[asset_id] = []
        
        self.comments[asset_id].append(comment)
        return comment
    
    def resolve_comment(self, comment_id: str) -> dict:
        for asset_id, comments in self.comments.items():
            for comment in comments:
                if comment["id"] == comment_id:
                    comment["resolved"] = True
                    comment["resolved_at"] = datetime.utcnow().isoformat()
                    return comment
        
        raise ValueError("Comment not found")
    
    def get_comments(self, asset_id: str) -> List[dict]:
        return self.comments.get(asset_id, [])


comments = CommentManager()


# =========================================================================
# Activity Feed
# =========================================================================

class ActivityFeed:
    """Track and display team activity"""
    
    def __init__(self):
        self.activities: List[dict] = []
        self._counter = 0
    
    def log_activity(
        self,
        user_id: str,
        action: str,
        entity_type: str,
        entity_id: str,
        details: dict = None
    ) -> dict:
        self._counter += 1
        
        activity = {
            "id": f"activity_{self._counter}",
            "user_id": user_id,
            "action": action,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "details": details or {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.activities.append(activity)
        return activity
    
    def get_feed(
        self,
        workspace_id: str = None,
        user_id: str = None,
        limit: int = 50
    ) -> List[dict]:
        feed = self.activities
        
        if user_id:
            feed = [a for a in feed if a["user_id"] == user_id]
        
        return feed[-limit:]


activity_feed = ActivityFeed()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/workspaces")
async def create_workspace(
    name: str = Query(...),
    description: str = Query(default=""),
    owner_id: str = Query(...)
):
    """Create workspace"""
    return workspaces.create_workspace(name, description, owner_id)


@router.get("/workspaces")
async def list_workspaces():
    """List workspaces"""
    return {"workspaces": list(workspaces.workspaces.values())}


@router.get("/workspaces/{workspace_id}")
async def get_workspace(workspace_id: str):
    """Get workspace details"""
    try:
        return workspaces.get_workspace(workspace_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/workspaces/{workspace_id}/members")
async def add_member(
    workspace_id: str,
    user_id: str = Query(...),
    role: str = Query(default="member")
):
    """Add member to workspace"""
    try:
        return workspaces.add_member(workspace_id, user_id, role)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/assets")
async def create_asset(
    workspace_id: str = Query(...),
    asset_type: AssetType = Query(...),
    name: str = Query(...),
    content: Any = None,
    created_by: str = Query(...)
):
    """Create shared asset"""
    return assets.create_asset(workspace_id, asset_type.value, name, content, created_by)


@router.get("/assets")
async def list_assets(workspace_id: Optional[str] = Query(default=None)):
    """List assets"""
    result = list(assets.assets.values())
    if workspace_id:
        result = [a for a in result if a["workspace_id"] == workspace_id]
    return {"assets": result}


@router.put("/assets/{asset_id}")
async def update_asset(asset_id: str, content: Any = None, updated_by: str = Query(...)):
    """Update asset"""
    try:
        return assets.update_asset(asset_id, content, updated_by)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/assets/{asset_id}/share")
async def share_asset(
    asset_id: str,
    user_id: str = Query(...),
    permission: PermissionLevel = Query(...)
):
    """Share asset"""
    try:
        return assets.share_asset(asset_id, user_id, permission.value)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/assets/{asset_id}/versions")
async def get_asset_versions(asset_id: str):
    """Get asset version history"""
    return {"versions": assets.get_versions(asset_id)}


@router.post("/assets/{asset_id}/comments")
async def add_comment(
    asset_id: str,
    user_id: str = Query(...),
    content: str = Query(...),
    parent_id: Optional[str] = Query(default=None)
):
    """Add comment"""
    return comments.add_comment(asset_id, user_id, content, parent_id)


@router.get("/assets/{asset_id}/comments")
async def get_comments(asset_id: str):
    """Get asset comments"""
    return {"comments": comments.get_comments(asset_id)}


@router.post("/comments/{comment_id}/resolve")
async def resolve_comment(comment_id: str):
    """Resolve comment"""
    try:
        return comments.resolve_comment(comment_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/activity")
async def get_activity_feed(
    workspace_id: Optional[str] = Query(default=None),
    user_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50)
):
    """Get activity feed"""
    return {"activities": activity_feed.get_feed(workspace_id, user_id, limit)}
