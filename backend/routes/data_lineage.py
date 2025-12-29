"""
Data Lineage & Catalog Routes for ACA DataHub
Column-level lineage, impact analysis, data catalog, and metadata management
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import random

router = APIRouter(prefix="/lineage", tags=["Data Lineage"])


# =========================================================================
# Models
# =========================================================================

class AssetType(str, Enum):
    TABLE = "table"
    VIEW = "view"
    COLUMN = "column"
    DATASET = "dataset"
    REPORT = "report"
    DASHBOARD = "dashboard"
    PIPELINE = "pipeline"


class DataQualityDimension(str, Enum):
    ACCURACY = "accuracy"
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"


# =========================================================================
# Data Catalog
# =========================================================================

class DataCatalog:
    """Centralized data catalog and metadata repository"""
    
    def __init__(self):
        self.assets: Dict[str, dict] = {}
        self.lineage_edges: List[dict] = []
        self.glossary: Dict[str, dict] = {}
        self.tags: Dict[str, List[str]] = {}
        self.owners: Dict[str, dict] = {}
        self._counter = 0
        self._init_sample_data()
    
    def _init_sample_data(self):
        """Initialize sample catalog data"""
        tables = [
            {"name": "leads", "columns": ["id", "email", "first_name", "last_name", "score", "state", "income"]},
            {"name": "populations", "columns": ["id", "name", "criteria", "count", "created_at"]},
            {"name": "campaigns", "columns": ["id", "name", "type", "sent_count", "open_rate"]},
            {"name": "users", "columns": ["id", "email", "role", "organization_id"]}
        ]
        
        for table in tables:
            asset_id = f"asset_{table['name']}"
            self.assets[asset_id] = {
                "id": asset_id,
                "name": table["name"],
                "type": AssetType.TABLE.value,
                "columns": table["columns"],
                "description": f"Table containing {table['name']} data",
                "owner": "data_team",
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "quality_score": round(random.uniform(0.7, 0.99), 2),
                "usage_count": random.randint(100, 5000),
                "tags": ["core", "production"]
            }
            
            # Add column-level assets
            for col in table["columns"]:
                col_id = f"asset_{table['name']}.{col}"
                self.assets[col_id] = {
                    "id": col_id,
                    "name": col,
                    "type": AssetType.COLUMN.value,
                    "parent": asset_id,
                    "data_type": self._infer_data_type(col),
                    "nullable": col != "id",
                    "pii": col in ["email", "first_name", "last_name"],
                    "description": f"Column {col} in {table['name']} table"
                }
        
        # Add lineage edges
        self.lineage_edges = [
            {"source": "asset_leads", "target": "asset_populations", "type": "derives"},
            {"source": "asset_leads", "target": "asset_campaigns", "type": "uses"},
            {"source": "asset_populations", "target": "asset_campaigns", "type": "feeds"}
        ]
    
    def _infer_data_type(self, column_name: str) -> str:
        if column_name == "id":
            return "string"
        elif column_name in ["score", "count", "income"]:
            return "integer"
        elif column_name in ["open_rate"]:
            return "float"
        elif column_name in ["created_at", "updated_at"]:
            return "timestamp"
        else:
            return "string"
    
    def register_asset(
        self,
        name: str,
        asset_type: str,
        description: str = None,
        owner: str = None,
        tags: List[str] = None
    ) -> dict:
        self._counter += 1
        asset_id = f"asset_{self._counter}"
        
        asset = {
            "id": asset_id,
            "name": name,
            "type": asset_type,
            "description": description or "",
            "owner": owner,
            "tags": tags or [],
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "quality_score": None,
            "usage_count": 0
        }
        
        self.assets[asset_id] = asset
        return asset
    
    def get_asset(self, asset_id: str) -> Optional[dict]:
        return self.assets.get(asset_id)
    
    def search_assets(
        self,
        query: str = None,
        asset_type: str = None,
        tags: List[str] = None,
        owner: str = None
    ) -> List[dict]:
        results = list(self.assets.values())
        
        if query:
            query_lower = query.lower()
            results = [a for a in results if 
                      query_lower in a.get("name", "").lower() or
                      query_lower in a.get("description", "").lower()]
        
        if asset_type:
            results = [a for a in results if a.get("type") == asset_type]
        
        if tags:
            results = [a for a in results if 
                      any(t in a.get("tags", []) for t in tags)]
        
        if owner:
            results = [a for a in results if a.get("owner") == owner]
        
        return results
    
    def add_lineage(self, source_id: str, target_id: str, lineage_type: str = "derives") -> dict:
        edge = {
            "source": source_id,
            "target": target_id,
            "type": lineage_type,
            "created_at": datetime.utcnow().isoformat()
        }
        self.lineage_edges.append(edge)
        return edge
    
    def get_upstream(self, asset_id: str, depth: int = 3) -> List[dict]:
        """Get upstream dependencies"""
        upstream = []
        current = [asset_id]
        
        for d in range(depth):
            next_level = []
            for edge in self.lineage_edges:
                if edge["target"] in current:
                    source = self.assets.get(edge["source"])
                    if source:
                        upstream.append({
                            "asset": source,
                            "edge_type": edge["type"],
                            "depth": d + 1
                        })
                        next_level.append(edge["source"])
            current = next_level
            if not current:
                break
        
        return upstream
    
    def get_downstream(self, asset_id: str, depth: int = 3) -> List[dict]:
        """Get downstream impacts"""
        downstream = []
        current = [asset_id]
        
        for d in range(depth):
            next_level = []
            for edge in self.lineage_edges:
                if edge["source"] in current:
                    target = self.assets.get(edge["target"])
                    if target:
                        downstream.append({
                            "asset": target,
                            "edge_type": edge["type"],
                            "depth": d + 1
                        })
                        next_level.append(edge["target"])
            current = next_level
            if not current:
                break
        
        return downstream
    
    def impact_analysis(self, asset_id: str) -> dict:
        """Analyze impact of changes to an asset"""
        downstream = self.get_downstream(asset_id)
        
        impact = {
            "asset_id": asset_id,
            "impacted_assets": len(downstream),
            "by_type": {},
            "critical_paths": [],
            "estimated_blast_radius": len(downstream)
        }
        
        for item in downstream:
            asset_type = item["asset"].get("type", "unknown")
            impact["by_type"][asset_type] = impact["by_type"].get(asset_type, 0) + 1
        
        # Identify critical paths (dashboards, reports)
        for item in downstream:
            if item["asset"].get("type") in ["dashboard", "report"]:
                impact["critical_paths"].append({
                    "asset": item["asset"]["name"],
                    "depth": item["depth"]
                })
        
        return impact
    
    def add_glossary_term(
        self,
        term: str,
        definition: str,
        domain: str = None,
        related_assets: List[str] = None
    ) -> dict:
        term_entry = {
            "term": term,
            "definition": definition,
            "domain": domain,
            "related_assets": related_assets or [],
            "created_at": datetime.utcnow().isoformat()
        }
        self.glossary[term.lower()] = term_entry
        return term_entry
    
    def get_glossary(self, domain: str = None) -> List[dict]:
        terms = list(self.glossary.values())
        if domain:
            terms = [t for t in terms if t.get("domain") == domain]
        return terms
    
    def update_quality_score(
        self,
        asset_id: str,
        scores: Dict[str, float]
    ) -> dict:
        if asset_id not in self.assets:
            raise ValueError("Asset not found")
        
        # Calculate overall score
        overall = sum(scores.values()) / len(scores) if scores else 0
        
        self.assets[asset_id]["quality_score"] = round(overall, 3)
        self.assets[asset_id]["quality_dimensions"] = scores
        self.assets[asset_id]["quality_updated_at"] = datetime.utcnow().isoformat()
        
        return self.assets[asset_id]


catalog = DataCatalog()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/assets")
async def register_asset(
    name: str = Query(...),
    asset_type: AssetType = Query(...),
    description: str = Query(default=""),
    owner: Optional[str] = Query(default=None),
    tags: List[str] = Query(default=[])
):
    """Register a new data asset"""
    asset = catalog.register_asset(name, asset_type.value, description, owner, tags)
    return {"success": True, "asset": asset}


@router.get("/assets")
async def search_assets(
    query: Optional[str] = Query(default=None),
    asset_type: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
    limit: int = Query(default=50)
):
    """Search data assets"""
    results = catalog.search_assets(query, asset_type, None, owner)
    return {"assets": results[:limit], "total": len(results)}


@router.get("/assets/{asset_id}")
async def get_asset(asset_id: str):
    """Get asset details"""
    asset = catalog.get_asset(asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    return asset


@router.post("/lineage")
async def add_lineage_edge(
    source_id: str = Query(...),
    target_id: str = Query(...),
    lineage_type: str = Query(default="derives")
):
    """Add lineage relationship between assets"""
    edge = catalog.add_lineage(source_id, target_id, lineage_type)
    return {"success": True, "edge": edge}


@router.get("/lineage/edges")
async def get_all_lineage():
    """Get all lineage edges"""
    return {"edges": catalog.lineage_edges}


@router.get("/lineage/{asset_id}/upstream")
async def get_upstream_lineage(asset_id: str, depth: int = Query(default=3)):
    """Get upstream dependencies"""
    return {"upstream": catalog.get_upstream(asset_id, depth)}


@router.get("/lineage/{asset_id}/downstream")
async def get_downstream_lineage(asset_id: str, depth: int = Query(default=3)):
    """Get downstream impacts"""
    return {"downstream": catalog.get_downstream(asset_id, depth)}


@router.get("/impact/{asset_id}")
async def analyze_impact(asset_id: str):
    """Analyze impact of changes to asset"""
    return catalog.impact_analysis(asset_id)


@router.post("/glossary")
async def add_glossary_term(
    term: str = Query(...),
    definition: str = Query(...),
    domain: Optional[str] = Query(default=None)
):
    """Add glossary term"""
    entry = catalog.add_glossary_term(term, definition, domain)
    return {"success": True, "term": entry}


@router.get("/glossary")
async def get_glossary(domain: Optional[str] = Query(default=None)):
    """Get business glossary"""
    return {"terms": catalog.get_glossary(domain)}


@router.post("/assets/{asset_id}/quality")
async def update_data_quality(
    asset_id: str,
    accuracy: float = Query(default=0, ge=0, le=1),
    completeness: float = Query(default=0, ge=0, le=1),
    consistency: float = Query(default=0, ge=0, le=1),
    timeliness: float = Query(default=0, ge=0, le=1)
):
    """Update data quality scores"""
    try:
        scores = {
            "accuracy": accuracy,
            "completeness": completeness,
            "consistency": consistency,
            "timeliness": timeliness
        }
        asset = catalog.update_quality_score(asset_id, scores)
        return {"success": True, "asset": asset}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/stats")
async def get_catalog_stats():
    """Get catalog statistics"""
    assets = list(catalog.assets.values())
    
    by_type = {}
    for a in assets:
        t = a.get("type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1
    
    pii_count = sum(1 for a in assets if a.get("pii"))
    
    return {
        "total_assets": len(assets),
        "assets_by_type": by_type,
        "lineage_edges": len(catalog.lineage_edges),
        "glossary_terms": len(catalog.glossary),
        "pii_assets": pii_count,
        "avg_quality_score": round(
            sum(a.get("quality_score", 0) for a in assets if a.get("quality_score")) /
            max(sum(1 for a in assets if a.get("quality_score")), 1), 3
        )
    }
