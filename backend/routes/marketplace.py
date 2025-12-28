"""
Data Marketplace Routes for ACA DataHub
Data catalog, dataset sharing, subscriptions, and cross-org data exchange
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

router = APIRouter(prefix="/marketplace", tags=["Data Marketplace"])


# =========================================================================
# Models
# =========================================================================

class DatasetVisibility(str, Enum):
    PRIVATE = "private"
    ORGANIZATION = "organization"
    PUBLIC = "public"


class DataQualityBadge(str, Enum):
    VERIFIED = "verified"
    COMPLETE = "complete"
    RECENT = "recent"
    POPULAR = "popular"


class DatasetCreate(BaseModel):
    name: str
    description: str
    category: str
    tags: List[str] = []
    visibility: DatasetVisibility = DatasetVisibility.PRIVATE
    schema_fields: List[Dict[str, str]] = []
    sample_size: int = 0
    price: float = 0.0  # 0 = free


# =========================================================================
# Marketplace Store
# =========================================================================

class MarketplaceStore:
    """Data catalog and marketplace"""
    
    def __init__(self):
        self.datasets: Dict[str, dict] = {}
        self.subscriptions: Dict[str, dict] = {}
        self.requests: Dict[str, dict] = {}
        self._ds_counter = 0
        self._sub_counter = 0
        self._req_counter = 0
        self._init_sample_datasets()
    
    def _init_sample_datasets(self):
        samples = [
            {
                "name": "ACA Qualifying Population 2024",
                "description": "Pre-filtered population of ACA-qualifying individuals with verified contact info",
                "category": "Healthcare",
                "tags": ["aca", "healthcare", "leads"],
                "visibility": "public",
                "schema_fields": [
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "income_range", "type": "string"},
                    {"name": "age", "type": "integer"},
                    {"name": "state", "type": "string"}
                ],
                "sample_size": 50000,
                "price": 0,
                "badges": ["verified", "complete"],
                "owner_org": "ACA DataHub"
            },
            {
                "name": "US Census Demographics 2023",
                "description": "Enhanced census data with demographic enrichment",
                "category": "Demographics",
                "tags": ["census", "demographics", "usa"],
                "visibility": "public",
                "schema_fields": [
                    {"name": "zip_code", "type": "string"},
                    {"name": "population", "type": "integer"},
                    {"name": "median_income", "type": "float"},
                    {"name": "age_distribution", "type": "object"}
                ],
                "sample_size": 41000,
                "price": 0,
                "badges": ["verified", "popular"],
                "owner_org": "Public Data"
            }
        ]
        
        for s in samples:
            self.publish(s)
    
    def publish(self, data: dict) -> dict:
        self._ds_counter += 1
        dataset_id = f"ds_{self._ds_counter}"
        
        dataset = {
            "id": dataset_id,
            "status": "published",
            "version": "1.0.0",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "download_count": 0,
            "subscriber_count": 0,
            "rating": 0,
            "review_count": 0,
            **data
        }
        
        self.datasets[dataset_id] = dataset
        return dataset
    
    def get(self, dataset_id: str) -> Optional[dict]:
        return self.datasets.get(dataset_id)
    
    def search(
        self,
        query: str = None,
        category: str = None,
        tags: List[str] = None,
        visibility: str = None,
        min_quality: int = None
    ) -> List[dict]:
        datasets = list(self.datasets.values())
        
        if query:
            query_lower = query.lower()
            datasets = [d for d in datasets if 
                       query_lower in d.get("name", "").lower() or
                       query_lower in d.get("description", "").lower()]
        
        if category:
            datasets = [d for d in datasets if d.get("category") == category]
        
        if tags:
            datasets = [d for d in datasets if 
                       any(t in d.get("tags", []) for t in tags)]
        
        if visibility:
            datasets = [d for d in datasets if d.get("visibility") == visibility]
        
        return sorted(datasets, key=lambda x: x.get("download_count", 0), reverse=True)
    
    def subscribe(self, dataset_id: str, user_id: str, org_id: str = None) -> dict:
        self._sub_counter += 1
        sub_id = f"sub_{self._sub_counter}"
        
        subscription = {
            "id": sub_id,
            "dataset_id": dataset_id,
            "user_id": user_id,
            "org_id": org_id,
            "status": "active",
            "created_at": datetime.utcnow().isoformat(),
            "last_sync": None
        }
        
        self.subscriptions[sub_id] = subscription
        
        # Update dataset stats
        if dataset_id in self.datasets:
            self.datasets[dataset_id]["subscriber_count"] += 1
        
        return subscription
    
    def unsubscribe(self, sub_id: str) -> bool:
        if sub_id in self.subscriptions:
            self.subscriptions[sub_id]["status"] = "cancelled"
            return True
        return False
    
    def get_user_subscriptions(self, user_id: str) -> List[dict]:
        return [s for s in self.subscriptions.values() 
                if s.get("user_id") == user_id and s.get("status") == "active"]
    
    def create_request(self, data: dict) -> dict:
        self._req_counter += 1
        req_id = f"req_{self._req_counter}"
        
        request = {
            "id": req_id,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            **data
        }
        
        self.requests[req_id] = request
        return request
    
    def list_requests(self, status: str = None) -> List[dict]:
        requests = list(self.requests.values())
        if status:
            requests = [r for r in requests if r.get("status") == status]
        return requests


marketplace_store = MarketplaceStore()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/catalog")
async def browse_catalog(
    query: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    tags: Optional[List[str]] = Query(default=None),
    visibility: Optional[str] = Query(default=None)
):
    """Browse data catalog"""
    datasets = marketplace_store.search(query, category, tags, visibility)
    return {
        "datasets": datasets,
        "total": len(datasets),
        "categories": ["Healthcare", "Demographics", "Financial", "Geographic", "Marketing"]
    }


@router.get("/catalog/{dataset_id}")
async def get_dataset(dataset_id: str):
    """Get dataset details"""
    dataset = marketplace_store.get(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


@router.post("/publish")
async def publish_dataset(dataset: DatasetCreate):
    """Publish a dataset to the marketplace"""
    result = marketplace_store.publish(dataset.dict())
    return {"success": True, "dataset": result}


@router.put("/catalog/{dataset_id}")
async def update_dataset(dataset_id: str, updates: dict):
    """Update dataset metadata"""
    if dataset_id not in marketplace_store.datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    marketplace_store.datasets[dataset_id].update(updates)
    marketplace_store.datasets[dataset_id]["updated_at"] = datetime.utcnow().isoformat()
    
    return {"success": True, "dataset": marketplace_store.datasets[dataset_id]}


@router.delete("/catalog/{dataset_id}")
async def unpublish_dataset(dataset_id: str):
    """Unpublish a dataset"""
    if dataset_id not in marketplace_store.datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    marketplace_store.datasets[dataset_id]["status"] = "unpublished"
    return {"success": True}


@router.post("/subscribe/{dataset_id}")
async def subscribe_to_dataset(
    dataset_id: str,
    user_id: str = Query(...),
    org_id: Optional[str] = Query(default=None)
):
    """Subscribe to a dataset"""
    dataset = marketplace_store.get(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    subscription = marketplace_store.subscribe(dataset_id, user_id, org_id)
    return {"success": True, "subscription": subscription}


@router.delete("/subscriptions/{sub_id}")
async def unsubscribe(sub_id: str):
    """Unsubscribe from a dataset"""
    if not marketplace_store.unsubscribe(sub_id):
        raise HTTPException(status_code=404, detail="Subscription not found")
    return {"success": True}


@router.get("/subscriptions")
async def list_subscriptions(user_id: str = Query(...)):
    """List user's subscriptions"""
    return {"subscriptions": marketplace_store.get_user_subscriptions(user_id)}


@router.post("/request")
async def request_dataset(
    title: str = Query(...),
    description: str = Query(...),
    category: str = Query(...),
    requester_id: str = Query(...)
):
    """Request a new dataset"""
    request = marketplace_store.create_request({
        "title": title,
        "description": description,
        "category": category,
        "requester_id": requester_id
    })
    return {"success": True, "request": request}


@router.get("/requests")
async def list_data_requests(status: Optional[str] = Query(default=None)):
    """List data requests"""
    return {"requests": marketplace_store.list_requests(status)}


@router.get("/{dataset_id}/schema")
async def get_dataset_schema(dataset_id: str):
    """Get dataset schema"""
    dataset = marketplace_store.get(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    return {
        "dataset_id": dataset_id,
        "name": dataset.get("name"),
        "schema_fields": dataset.get("schema_fields", []),
        "version": dataset.get("version")
    }


@router.get("/{dataset_id}/preview")
async def preview_dataset(dataset_id: str, rows: int = Query(default=5, le=20)):
    """Preview dataset sample data"""
    dataset = marketplace_store.get(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Generate sample preview data
    sample = []
    for i in range(rows):
        row = {}
        for field in dataset.get("schema_fields", []):
            if field["type"] == "string":
                row[field["name"]] = f"sample_{i}"
            elif field["type"] == "integer":
                row[field["name"]] = i * 10
            elif field["type"] == "float":
                row[field["name"]] = i * 10.5
            else:
                row[field["name"]] = None
        sample.append(row)
    
    return {
        "dataset_id": dataset_id,
        "sample": sample,
        "total_rows": dataset.get("sample_size", 0)
    }


@router.post("/{dataset_id}/version")
async def create_version(dataset_id: str, version: str, changelog: str = None):
    """Create a new version of the dataset"""
    dataset = marketplace_store.get(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    dataset["version"] = version
    dataset["updated_at"] = datetime.utcnow().isoformat()
    
    if "version_history" not in dataset:
        dataset["version_history"] = []
    
    dataset["version_history"].append({
        "version": version,
        "created_at": datetime.utcnow().isoformat(),
        "changelog": changelog
    })
    
    return {"success": True, "version": version}
