"""
Distributed Computing Routes for ACA DataHub
Distributed queries, sharding, replication, and fault tolerance
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import hashlib
import asyncio

router = APIRouter(prefix="/distributed", tags=["Distributed Computing"])


# =========================================================================
# Models
# =========================================================================

class NodeStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OFFLINE = "offline"


class ReplicationStrategy(str, Enum):
    SYNC = "synchronous"
    ASYNC = "asynchronous"
    SEMI_SYNC = "semi_synchronous"


class ConsistencyLevel(str, Enum):
    ONE = "one"
    QUORUM = "quorum"
    ALL = "all"


# =========================================================================
# Cluster Manager
# =========================================================================

class ClusterManager:
    """Manages distributed cluster nodes"""
    
    def __init__(self):
        self.nodes: Dict[str, dict] = {}
        self.shards: Dict[str, dict] = {}
        self.partitions: Dict[str, List[str]] = {}
        self._counter = 0
        self._init_cluster()
    
    def _init_cluster(self):
        """Initialize simulated cluster"""
        regions = ["us-east-1", "us-west-2", "eu-west-1"]
        
        for i, region in enumerate(regions):
            for j in range(2):  # 2 nodes per region
                node_id = f"node_{i*2+j+1}"
                self.nodes[node_id] = {
                    "id": node_id,
                    "region": region,
                    "zone": f"{region}a" if j == 0 else f"{region}b",
                    "status": NodeStatus.HEALTHY.value,
                    "cpu_usage": round(random.uniform(20, 60), 1),
                    "memory_usage": round(random.uniform(30, 70), 1),
                    "disk_usage": round(random.uniform(40, 80), 1),
                    "connections": random.randint(10, 100),
                    "last_heartbeat": datetime.utcnow().isoformat()
                }
        
        # Initialize shards
        for i in range(4):
            shard_id = f"shard_{i+1}"
            self.shards[shard_id] = {
                "id": shard_id,
                "primary": f"node_{(i % 6) + 1}",
                "replicas": [f"node_{((i+1) % 6) + 1}", f"node_{((i+2) % 6) + 1}"],
                "key_range": f"{i*25}-{(i+1)*25-1}",
                "record_count": random.randint(10000, 100000)
            }
    
    def get_node(self, node_id: str) -> Optional[dict]:
        return self.nodes.get(node_id)
    
    def list_nodes(self, region: str = None, status: str = None) -> List[dict]:
        nodes = list(self.nodes.values())
        
        if region:
            nodes = [n for n in nodes if n["region"] == region]
        if status:
            nodes = [n for n in nodes if n["status"] == status]
        
        return nodes
    
    def get_shard_for_key(self, key: str) -> str:
        """Consistent hashing to find shard for key"""
        hash_val = int(hashlib.md5(key.encode()).hexdigest()[:8], 16)
        shard_index = hash_val % len(self.shards)
        return f"shard_{shard_index + 1}"
    
    def rebalance_shards(self) -> dict:
        """Rebalance shards across nodes"""
        total_records = sum(s["record_count"] for s in self.shards.values())
        target_per_shard = total_records // len(self.shards)
        
        moves = []
        for shard_id, shard in self.shards.items():
            diff = shard["record_count"] - target_per_shard
            if abs(diff) > target_per_shard * 0.2:  # More than 20% imbalance
                moves.append({
                    "shard": shard_id,
                    "action": "split" if diff > 0 else "merge",
                    "records_affected": abs(diff)
                })
        
        return {
            "status": "completed",
            "target_per_shard": target_per_shard,
            "moves": moves
        }
    
    def failover(self, node_id: str) -> dict:
        """Handle node failure and promote replica"""
        if node_id not in self.nodes:
            raise ValueError("Node not found")
        
        self.nodes[node_id]["status"] = NodeStatus.OFFLINE.value
        
        # Find shards with this node as primary
        affected_shards = []
        for shard_id, shard in self.shards.items():
            if shard["primary"] == node_id:
                # Promote first replica
                if shard["replicas"]:
                    new_primary = shard["replicas"][0]
                    shard["replicas"] = shard["replicas"][1:] + [node_id]
                    shard["primary"] = new_primary
                    affected_shards.append({
                        "shard": shard_id,
                        "old_primary": node_id,
                        "new_primary": new_primary
                    })
        
        return {
            "triggered_by": node_id,
            "affected_shards": affected_shards,
            "timestamp": datetime.utcnow().isoformat()
        }


cluster_manager = ClusterManager()


# =========================================================================
# Distributed Query Engine
# =========================================================================

class DistributedQueryEngine:
    """Execute queries across distributed nodes"""
    
    def __init__(self):
        self.query_cache: Dict[str, dict] = {}
        self._counter = 0
    
    async def execute_distributed(
        self,
        query: str,
        consistency: str = "quorum",
        timeout_ms: int = 5000
    ) -> dict:
        self._counter += 1
        query_id = f"dq_{self._counter}"
        
        # Simulate distributed query execution
        start_time = datetime.utcnow()
        
        # Determine which shards to query
        shards_to_query = list(cluster_manager.shards.keys())
        
        # Simulate parallel execution
        shard_results = []
        for shard_id in shards_to_query:
            shard = cluster_manager.shards[shard_id]
            nodes_queried = [shard["primary"]]
            
            if consistency == "quorum":
                nodes_queried.extend(shard["replicas"][:1])
            elif consistency == "all":
                nodes_queried.extend(shard["replicas"])
            
            shard_results.append({
                "shard": shard_id,
                "nodes": nodes_queried,
                "rows_scanned": random.randint(100, 5000),
                "rows_returned": random.randint(10, 100),
                "latency_ms": round(random.uniform(5, 50), 2)
            })
        
        total_rows = sum(r["rows_returned"] for r in shard_results)
        total_latency = max(r["latency_ms"] for r in shard_results)
        
        result = {
            "query_id": query_id,
            "query": query,
            "consistency": consistency,
            "shards_queried": len(shards_to_query),
            "shard_results": shard_results,
            "total_rows": total_rows,
            "latency_ms": total_latency,
            "executed_at": start_time.isoformat()
        }
        
        # Cache result
        self.query_cache[query_id] = result
        
        return result
    
    def explain_query(self, query: str) -> dict:
        """Explain distributed query execution plan"""
        shards = list(cluster_manager.shards.keys())
        
        return {
            "query": query,
            "plan": {
                "type": "distributed_scan",
                "shards": shards,
                "estimated_rows": sum(s["record_count"] for s in cluster_manager.shards.values()),
                "parallelism": len(shards),
                "consistency": "quorum",
                "steps": [
                    {"step": 1, "operation": "scatter", "description": "Send query to all shards"},
                    {"step": 2, "operation": "local_scan", "description": "Execute on each shard"},
                    {"step": 3, "operation": "gather", "description": "Collect results"},
                    {"step": 4, "operation": "merge_sort", "description": "Merge and sort results"}
                ]
            }
        }


query_engine = DistributedQueryEngine()


# =========================================================================
# Replication Manager
# =========================================================================

class ReplicationManager:
    """Manages data replication across nodes"""
    
    def __init__(self):
        self.replication_config: Dict[str, dict] = {}
        self.replication_lag: Dict[str, int] = {}
    
    def configure_replication(
        self,
        shard_id: str,
        strategy: str,
        replicas: int = 2
    ) -> dict:
        config = {
            "shard_id": shard_id,
            "strategy": strategy,
            "replica_count": replicas,
            "configured_at": datetime.utcnow().isoformat()
        }
        
        self.replication_config[shard_id] = config
        return config
    
    def get_replication_status(self, shard_id: str) -> dict:
        shard = cluster_manager.shards.get(shard_id)
        if not shard:
            return {}
        
        replicas_status = []
        for replica_id in shard["replicas"]:
            node = cluster_manager.nodes.get(replica_id, {})
            lag = random.randint(0, 50)  # Simulated lag in ms
            
            replicas_status.append({
                "node_id": replica_id,
                "status": node.get("status", "unknown"),
                "lag_ms": lag,
                "in_sync": lag < 10
            })
        
        return {
            "shard_id": shard_id,
            "primary": shard["primary"],
            "replicas": replicas_status,
            "strategy": self.replication_config.get(shard_id, {}).get("strategy", "async")
        }


replication_manager = ReplicationManager()


# =========================================================================
# Load Balancer
# =========================================================================

class LoadBalancer:
    """Distributes queries across nodes"""
    
    def __init__(self):
        self.algorithms = ["round_robin", "least_connections", "weighted", "random"]
        self.current_index = 0
        self.weights: Dict[str, float] = {}
    
    def select_node(
        self,
        shard_id: str,
        algorithm: str = "least_connections"
    ) -> str:
        shard = cluster_manager.shards.get(shard_id)
        if not shard:
            return None
        
        candidates = [shard["primary"]] + shard["replicas"]
        healthy = [n for n in candidates if 
                  cluster_manager.nodes.get(n, {}).get("status") == NodeStatus.HEALTHY.value]
        
        if not healthy:
            return shard["primary"]  # Fallback
        
        if algorithm == "round_robin":
            self.current_index = (self.current_index + 1) % len(healthy)
            return healthy[self.current_index]
        
        elif algorithm == "least_connections":
            return min(healthy, key=lambda n: 
                      cluster_manager.nodes.get(n, {}).get("connections", 0))
        
        elif algorithm == "weighted":
            # Weight by CPU availability
            weights = {n: 100 - cluster_manager.nodes.get(n, {}).get("cpu_usage", 50) 
                      for n in healthy}
            total = sum(weights.values())
            r = random.uniform(0, total)
            cumulative = 0
            for node, weight in weights.items():
                cumulative += weight
                if r <= cumulative:
                    return node
            return healthy[0]
        
        else:  # random
            return random.choice(healthy)


load_balancer = LoadBalancer()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/nodes")
async def list_nodes(
    region: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None)
):
    """List cluster nodes"""
    return {"nodes": cluster_manager.list_nodes(region, status)}


@router.get("/nodes/{node_id}")
async def get_node(node_id: str):
    """Get node details"""
    node = cluster_manager.get_node(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@router.get("/shards")
async def list_shards():
    """List data shards"""
    return {"shards": list(cluster_manager.shards.values())}


@router.get("/shards/{key}")
async def get_shard_for_key(key: str):
    """Get shard for a given key"""
    shard_id = cluster_manager.get_shard_for_key(key)
    return {"key": key, "shard": shard_id, "shard_info": cluster_manager.shards.get(shard_id)}


@router.post("/shards/rebalance")
async def rebalance_shards():
    """Rebalance shards across cluster"""
    return cluster_manager.rebalance_shards()


@router.post("/failover/{node_id}")
async def trigger_failover(node_id: str):
    """Trigger failover for a node"""
    try:
        return cluster_manager.failover(node_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/query")
async def execute_distributed_query(
    query: str = Query(...),
    consistency: ConsistencyLevel = Query(default=ConsistencyLevel.QUORUM),
    timeout_ms: int = Query(default=5000)
):
    """Execute distributed query"""
    result = await query_engine.execute_distributed(query, consistency.value, timeout_ms)
    return result


@router.get("/query/{query_id}/explain")
async def explain_query(query: str = Query(...)):
    """Explain query execution plan"""
    return query_engine.explain_query(query)


@router.post("/replication/configure")
async def configure_replication(
    shard_id: str = Query(...),
    strategy: ReplicationStrategy = Query(default=ReplicationStrategy.ASYNC),
    replicas: int = Query(default=2, ge=1, le=5)
):
    """Configure replication for shard"""
    return replication_manager.configure_replication(shard_id, strategy.value, replicas)


@router.get("/replication/{shard_id}/status")
async def get_replication_status(shard_id: str):
    """Get replication status for shard"""
    return replication_manager.get_replication_status(shard_id)


@router.get("/loadbalancer/select")
async def select_node(
    shard_id: str = Query(...),
    algorithm: str = Query(default="least_connections")
):
    """Select optimal node for query"""
    node = load_balancer.select_node(shard_id, algorithm)
    return {"shard_id": shard_id, "selected_node": node, "algorithm": algorithm}


@router.get("/cluster/health")
async def get_cluster_health():
    """Get overall cluster health"""
    nodes = cluster_manager.list_nodes()
    healthy = sum(1 for n in nodes if n["status"] == "healthy")
    
    return {
        "status": "healthy" if healthy == len(nodes) else "degraded",
        "total_nodes": len(nodes),
        "healthy_nodes": healthy,
        "total_shards": len(cluster_manager.shards),
        "avg_cpu": round(sum(n["cpu_usage"] for n in nodes) / len(nodes), 1),
        "avg_memory": round(sum(n["memory_usage"] for n in nodes) / len(nodes), 1)
    }
