"""
Global Deployment & Edge Routes for ACA DataHub
Multi-region deployment, edge computing, CDN, and geo-routing
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random

router = APIRouter(prefix="/global", tags=["Global Deployment"])


# =========================================================================
# Models
# =========================================================================

class RegionStatus(str, Enum):
    ACTIVE = "active"
    STANDBY = "standby"
    MAINTENANCE = "maintenance"
    OFFLINE = "offline"


class FailoverMode(str, Enum):
    AUTOMATIC = "automatic"
    MANUAL = "manual"


# =========================================================================
# Global Infrastructure
# =========================================================================

class GlobalInfrastructure:
    """Manages multi-region deployment"""
    
    def __init__(self):
        self.regions: Dict[str, dict] = {}
        self.edge_nodes: Dict[str, dict] = {}
        self.routing_rules: List[dict] = []
        self._init_regions()
        self._init_edge_nodes()
    
    def _init_regions(self):
        """Initialize regions"""
        self.regions = {
            "us-east-1": {
                "id": "us-east-1",
                "name": "US East (N. Virginia)",
                "status": RegionStatus.ACTIVE.value,
                "is_primary": True,
                "latency_baseline_ms": 20,
                "capacity_percent": 45,
                "instances": 12,
                "endpoints": ["api-east.aca-datahub.com"],
                "data_sovereignty": ["US", "CA"]
            },
            "us-west-2": {
                "id": "us-west-2",
                "name": "US West (Oregon)",
                "status": RegionStatus.ACTIVE.value,
                "is_primary": False,
                "latency_baseline_ms": 35,
                "capacity_percent": 30,
                "instances": 8,
                "endpoints": ["api-west.aca-datahub.com"],
                "data_sovereignty": ["US"]
            },
            "eu-west-1": {
                "id": "eu-west-1",
                "name": "EU (Ireland)",
                "status": RegionStatus.ACTIVE.value,
                "is_primary": False,
                "latency_baseline_ms": 80,
                "capacity_percent": 25,
                "instances": 6,
                "endpoints": ["api-eu.aca-datahub.com"],
                "data_sovereignty": ["EU", "UK", "GDPR"]
            },
            "ap-southeast-1": {
                "id": "ap-southeast-1",
                "name": "Asia Pacific (Singapore)",
                "status": RegionStatus.STANDBY.value,
                "is_primary": False,
                "latency_baseline_ms": 150,
                "capacity_percent": 0,
                "instances": 4,
                "endpoints": ["api-ap.aca-datahub.com"],
                "data_sovereignty": ["SG", "APAC"]
            }
        }
    
    def _init_edge_nodes(self):
        """Initialize edge computing nodes"""
        edge_locations = [
            ("edge-nyc", "New York", "US", 12),
            ("edge-lax", "Los Angeles", "US", 15),
            ("edge-ldn", "London", "UK", 18),
            ("edge-fra", "Frankfurt", "DE", 22),
            ("edge-tyo", "Tokyo", "JP", 120),
            ("edge-syd", "Sydney", "AU", 140)
        ]
        
        for eid, city, country, latency in edge_locations:
            self.edge_nodes[eid] = {
                "id": eid,
                "city": city,
                "country": country,
                "status": "active",
                "latency_ms": latency,
                "cache_hit_rate": round(random.uniform(0.85, 0.98), 2),
                "requests_per_second": random.randint(100, 1000),
                "cached_objects": random.randint(5000, 50000)
            }
    
    def get_optimal_region(self, client_ip: str, user_country: str = None) -> dict:
        """Determine optimal region based on latency and data sovereignty"""
        active_regions = [r for r in self.regions.values() 
                        if r["status"] == RegionStatus.ACTIVE.value]
        
        if not active_regions:
            # Fallback to primary
            primary = next((r for r in self.regions.values() if r["is_primary"]), None)
            return primary
        
        # Check data sovereignty requirements
        if user_country:
            if user_country in ["DE", "FR", "IT", "ES", "NL", "BE"]:
                # EU countries must use EU region for GDPR
                eu_region = self.regions.get("eu-west-1")
                if eu_region and eu_region["status"] == RegionStatus.ACTIVE.value:
                    return eu_region
        
        # Return lowest latency region
        return min(active_regions, key=lambda r: r["latency_baseline_ms"])
    
    def trigger_failover(self, from_region: str, to_region: str) -> dict:
        """Trigger regional failover"""
        if from_region not in self.regions or to_region not in self.regions:
            raise ValueError("Region not found")
        
        self.regions[from_region]["status"] = RegionStatus.STANDBY.value
        self.regions[to_region]["status"] = RegionStatus.ACTIVE.value
        
        if self.regions[from_region]["is_primary"]:
            self.regions[from_region]["is_primary"] = False
            self.regions[to_region]["is_primary"] = True
        
        return {
            "failover_id": f"fo_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            "from_region": from_region,
            "to_region": to_region,
            "triggered_at": datetime.utcnow().isoformat(),
            "status": "completed"
        }
    
    def get_region(self, region_id: str) -> Optional[dict]:
        return self.regions.get(region_id)
    
    def list_regions(self, status: str = None) -> List[dict]:
        regions = list(self.regions.values())
        if status:
            regions = [r for r in regions if r["status"] == status]
        return regions


global_infra = GlobalInfrastructure()


# =========================================================================
# CDN Manager
# =========================================================================

class CDNManager:
    """Manages content delivery network"""
    
    def __init__(self):
        self.cache_rules: List[dict] = []
        self.purge_history: List[dict] = []
    
    def add_cache_rule(
        self,
        path_pattern: str,
        ttl_seconds: int,
        edge_caching: bool = True
    ) -> dict:
        rule = {
            "id": f"rule_{len(self.cache_rules) + 1}",
            "path_pattern": path_pattern,
            "ttl_seconds": ttl_seconds,
            "edge_caching": edge_caching,
            "created_at": datetime.utcnow().isoformat()
        }
        self.cache_rules.append(rule)
        return rule
    
    def purge_cache(self, paths: List[str] = None, all_cache: bool = False) -> dict:
        purge = {
            "id": f"purge_{len(self.purge_history) + 1}",
            "paths": paths if not all_cache else ["*"],
            "all_cache": all_cache,
            "triggered_at": datetime.utcnow().isoformat(),
            "status": "completed",
            "nodes_purged": len(global_infra.edge_nodes)
        }
        self.purge_history.append(purge)
        return purge
    
    def get_cache_stats(self) -> dict:
        nodes = list(global_infra.edge_nodes.values())
        
        return {
            "total_nodes": len(nodes),
            "avg_hit_rate": round(sum(n["cache_hit_rate"] for n in nodes) / len(nodes), 3),
            "total_cached_objects": sum(n["cached_objects"] for n in nodes),
            "total_rps": sum(n["requests_per_second"] for n in nodes),
            "rules_count": len(self.cache_rules)
        }


cdn_manager = CDNManager()


# =========================================================================
# Data Sovereignty Manager
# =========================================================================

class DataSovereigntyManager:
    """Manages data residency requirements"""
    
    def __init__(self):
        self.policies: Dict[str, dict] = {
            "GDPR": {
                "id": "GDPR",
                "name": "EU General Data Protection Regulation",
                "countries": ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", 
                             "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU",
                             "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"],
                "allowed_regions": ["eu-west-1"],
                "data_transfer_rules": "SCCs or adequacy required"
            },
            "CCPA": {
                "id": "CCPA",
                "name": "California Consumer Privacy Act",
                "countries": ["US"],
                "states": ["CA"],
                "allowed_regions": ["us-east-1", "us-west-2"],
                "data_transfer_rules": "No cross-border restrictions"
            },
            "LGPD": {
                "id": "LGPD",
                "name": "Brazil Data Protection Law",
                "countries": ["BR"],
                "allowed_regions": ["us-east-1"],  # Via adequacy
                "data_transfer_rules": "ANPD authorization required"
            }
        }
    
    def check_compliance(self, user_country: str, target_region: str) -> dict:
        applicable_policies = []
        
        for policy_id, policy in self.policies.items():
            if user_country in policy.get("countries", []):
                applicable_policies.append(policy_id)
        
        if not applicable_policies:
            return {"compliant": True, "policies": [], "region": target_region}
        
        # Check if target region is allowed
        for policy_id in applicable_policies:
            policy = self.policies[policy_id]
            if target_region not in policy.get("allowed_regions", []):
                return {
                    "compliant": False,
                    "policies": applicable_policies,
                    "violation": f"{policy_id} requires data in {policy['allowed_regions']}",
                    "recommended_region": policy["allowed_regions"][0]
                }
        
        return {
            "compliant": True,
            "policies": applicable_policies,
            "region": target_region
        }


sovereignty_manager = DataSovereigntyManager()


# =========================================================================
# Status Dashboard
# =========================================================================

class GlobalStatusDashboard:
    """Global system status"""
    
    def __init__(self):
        self.incidents: List[dict] = []
        self.maintenance: List[dict] = []
    
    def get_status(self) -> dict:
        regions = global_infra.list_regions()
        edges = list(global_infra.edge_nodes.values())
        
        active_regions = sum(1 for r in regions if r["status"] == "active")
        active_edges = sum(1 for e in edges if e["status"] == "active")
        
        overall_status = "operational"
        if active_regions < len(regions):
            overall_status = "partial_outage"
        if active_regions == 0:
            overall_status = "major_outage"
        
        return {
            "status": overall_status,
            "updated_at": datetime.utcnow().isoformat(),
            "components": {
                "api": "operational",
                "database": "operational",
                "cdn": "operational" if active_edges > 0 else "degraded",
                "regions": f"{active_regions}/{len(regions)} active"
            },
            "regions": [
                {"id": r["id"], "name": r["name"], "status": r["status"]}
                for r in regions
            ],
            "active_incidents": len([i for i in self.incidents if not i.get("resolved")]),
            "upcoming_maintenance": len([m for m in self.maintenance if m.get("scheduled")])
        }
    
    def add_incident(self, title: str, severity: str, affected: List[str]) -> dict:
        incident = {
            "id": f"inc_{len(self.incidents) + 1}",
            "title": title,
            "severity": severity,
            "affected_components": affected,
            "started_at": datetime.utcnow().isoformat(),
            "resolved": False,
            "updates": []
        }
        self.incidents.append(incident)
        return incident


status_dashboard = GlobalStatusDashboard()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/regions")
async def list_regions(status: Optional[str] = Query(default=None)):
    """List all regions"""
    return {"regions": global_infra.list_regions(status)}


@router.get("/regions/{region_id}")
async def get_region(region_id: str):
    """Get region details"""
    region = global_infra.get_region(region_id)
    if not region:
        raise HTTPException(status_code=404, detail="Region not found")
    return region


@router.get("/routing")
async def get_optimal_route(
    client_ip: str = Query(default="0.0.0.0"),
    country: Optional[str] = Query(default=None)
):
    """Get optimal region for client"""
    region = global_infra.get_optimal_region(client_ip, country)
    return {
        "recommended_region": region["id"],
        "endpoint": region["endpoints"][0] if region.get("endpoints") else None,
        "expected_latency_ms": region["latency_baseline_ms"]
    }


@router.post("/failover")
async def trigger_failover(
    from_region: str = Query(...),
    to_region: str = Query(...)
):
    """Trigger regional failover"""
    try:
        return global_infra.trigger_failover(from_region, to_region)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/edge-nodes")
async def list_edge_nodes():
    """List edge computing nodes"""
    return {"edge_nodes": list(global_infra.edge_nodes.values())}


@router.get("/edge-nodes/{node_id}")
async def get_edge_node(node_id: str):
    """Get edge node details"""
    node = global_infra.edge_nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Edge node not found")
    return node


@router.post("/cdn/cache-rules")
async def add_cache_rule(
    path_pattern: str = Query(...),
    ttl_seconds: int = Query(default=3600),
    edge_caching: bool = Query(default=True)
):
    """Add CDN cache rule"""
    return cdn_manager.add_cache_rule(path_pattern, ttl_seconds, edge_caching)


@router.get("/cdn/cache-rules")
async def list_cache_rules():
    """List CDN cache rules"""
    return {"rules": cdn_manager.cache_rules}


@router.post("/cdn/purge")
async def purge_cache(
    paths: Optional[List[str]] = Query(default=None),
    all_cache: bool = Query(default=False)
):
    """Purge CDN cache"""
    return cdn_manager.purge_cache(paths, all_cache)


@router.get("/cdn/stats")
async def get_cdn_stats():
    """Get CDN statistics"""
    return cdn_manager.get_cache_stats()


@router.get("/sovereignty/check")
async def check_data_sovereignty(
    country: str = Query(...),
    region: str = Query(...)
):
    """Check data sovereignty compliance"""
    return sovereignty_manager.check_compliance(country, region)


@router.get("/sovereignty/policies")
async def list_sovereignty_policies():
    """List data sovereignty policies"""
    return {"policies": list(sovereignty_manager.policies.values())}


@router.get("/status")
async def get_global_status():
    """Get global system status"""
    return status_dashboard.get_status()


@router.post("/incidents")
async def report_incident(
    title: str = Query(...),
    severity: str = Query(default="medium"),
    affected: List[str] = Query(default=["api"])
):
    """Report an incident"""
    return status_dashboard.add_incident(title, severity, affected)
