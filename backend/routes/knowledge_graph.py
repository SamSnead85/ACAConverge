"""
Knowledge Graph & Semantic Routes for ACA DataHub
Entity extraction, relationship mapping, semantic search, and ontology management
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from enum import Enum
import random
import hashlib

router = APIRouter(prefix="/knowledge-graph", tags=["Knowledge Graph"])


# =========================================================================
# Models
# =========================================================================

class EntityType(str, Enum):
    PERSON = "person"
    ORGANIZATION = "organization"
    LOCATION = "location"
    PRODUCT = "product"
    CONCEPT = "concept"
    EVENT = "event"


class RelationType(str, Enum):
    IS_A = "is_a"
    HAS = "has"
    BELONGS_TO = "belongs_to"
    RELATED_TO = "related_to"
    WORKS_FOR = "works_for"
    LOCATED_IN = "located_in"
    INTERESTED_IN = "interested_in"


# =========================================================================
# Knowledge Graph Store
# =========================================================================

class KnowledgeGraphStore:
    """Graph-based knowledge storage"""
    
    def __init__(self):
        self.entities: Dict[str, dict] = {}
        self.relationships: List[dict] = []
        self.ontology: Dict[str, dict] = {}
        self._counter = 0
        self._init_ontology()
    
    def _init_ontology(self):
        """Initialize base ontology"""
        self.ontology = {
            "person": {
                "properties": ["name", "email", "phone", "age", "income"],
                "relationships": ["works_for", "located_in", "interested_in"]
            },
            "organization": {
                "properties": ["name", "industry", "size", "revenue"],
                "relationships": ["has", "located_in"]
            },
            "location": {
                "properties": ["name", "type", "coordinates", "state", "zip"],
                "relationships": ["belongs_to"]
            },
            "product": {
                "properties": ["name", "category", "price"],
                "relationships": ["belongs_to", "related_to"]
            },
            "concept": {
                "properties": ["name", "definition"],
                "relationships": ["is_a", "related_to"]
            }
        }
    
    def create_entity(
        self,
        entity_type: str,
        name: str,
        properties: dict = None,
        source: str = None
    ) -> dict:
        self._counter += 1
        entity_id = f"ent_{self._counter}"
        
        entity = {
            "id": entity_id,
            "type": entity_type,
            "name": name,
            "properties": properties or {},
            "source": source,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "embedding": self._generate_embedding(name, entity_type)
        }
        
        self.entities[entity_id] = entity
        return entity
    
    def _generate_embedding(self, text: str, context: str = "") -> List[float]:
        """Generate a simulated embedding vector"""
        combined = f"{text}:{context}"
        # Simulate 128-dim embedding
        random.seed(hash(combined) % (2**32))
        return [round(random.uniform(-1, 1), 4) for _ in range(128)]
    
    def get_entity(self, entity_id: str) -> Optional[dict]:
        return self.entities.get(entity_id)
    
    def search_entities(
        self,
        query: str = None,
        entity_type: str = None,
        limit: int = 10
    ) -> List[dict]:
        entities = list(self.entities.values())
        
        if entity_type:
            entities = [e for e in entities if e["type"] == entity_type]
        
        if query:
            query_lower = query.lower()
            entities = [
                e for e in entities
                if query_lower in e["name"].lower()
                or query_lower in str(e.get("properties", {})).lower()
            ]
        
        return entities[:limit]
    
    def create_relationship(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        properties: dict = None
    ) -> dict:
        if source_id not in self.entities:
            raise ValueError("Source entity not found")
        if target_id not in self.entities:
            raise ValueError("Target entity not found")
        
        self._counter += 1
        rel_id = f"rel_{self._counter}"
        
        relationship = {
            "id": rel_id,
            "source_id": source_id,
            "target_id": target_id,
            "type": relation_type,
            "properties": properties or {},
            "created_at": datetime.utcnow().isoformat()
        }
        
        self.relationships.append(relationship)
        return relationship
    
    def get_relationships(
        self,
        entity_id: str = None,
        relation_type: str = None
    ) -> List[dict]:
        rels = self.relationships
        
        if entity_id:
            rels = [
                r for r in rels
                if r["source_id"] == entity_id or r["target_id"] == entity_id
            ]
        
        if relation_type:
            rels = [r for r in rels if r["type"] == relation_type]
        
        return rels
    
    def get_neighbors(self, entity_id: str, depth: int = 1) -> dict:
        """Get entity neighbors up to specified depth"""
        if entity_id not in self.entities:
            return {}
        
        visited = {entity_id}
        neighbors = []
        current_level = [entity_id]
        
        for d in range(depth):
            next_level = []
            for eid in current_level:
                for rel in self.relationships:
                    if rel["source_id"] == eid and rel["target_id"] not in visited:
                        visited.add(rel["target_id"])
                        next_level.append(rel["target_id"])
                        neighbors.append({
                            "entity": self.entities.get(rel["target_id"]),
                            "relationship": rel,
                            "depth": d + 1
                        })
                    elif rel["target_id"] == eid and rel["source_id"] not in visited:
                        visited.add(rel["source_id"])
                        next_level.append(rel["source_id"])
                        neighbors.append({
                            "entity": self.entities.get(rel["source_id"]),
                            "relationship": rel,
                            "depth": d + 1
                        })
            current_level = next_level
        
        return {
            "entity": self.entities[entity_id],
            "neighbors": neighbors
        }
    
    def semantic_search(self, query: str, limit: int = 10) -> List[dict]:
        """Semantic similarity search using embeddings"""
        query_embedding = self._generate_embedding(query)
        
        results = []
        for entity in self.entities.values():
            # Calculate cosine similarity
            entity_emb = entity.get("embedding", [])
            if entity_emb:
                similarity = self._cosine_similarity(query_embedding, entity_emb)
                results.append({
                    "entity": entity,
                    "similarity": round(similarity, 4)
                })
        
        # Sort by similarity
        results.sort(key=lambda x: x["similarity"], reverse=True)
        return results[:limit]
    
    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        if len(a) != len(b):
            return 0
        
        dot_product = sum(x * y for x, y in zip(a, b))
        magnitude_a = sum(x * x for x in a) ** 0.5
        magnitude_b = sum(x * x for x in b) ** 0.5
        
        if magnitude_a == 0 or magnitude_b == 0:
            return 0
        
        return dot_product / (magnitude_a * magnitude_b)
    
    def extract_entities_from_text(self, text: str) -> List[dict]:
        """Extract entities from natural language text"""
        # Simulate NLP entity extraction
        extracted = []
        
        # Simple keyword-based extraction
        keywords = {
            "person": ["john", "jane", "user", "customer", "lead"],
            "location": ["georgia", "florida", "texas", "california", "new york"],
            "organization": ["company", "corp", "inc", "llc", "insurance"],
            "concept": ["aca", "healthcare", "insurance", "subsidy", "enrollment"]
        }
        
        text_lower = text.lower()
        
        for entity_type, words in keywords.items():
            for word in words:
                if word in text_lower:
                    extracted.append({
                        "text": word,
                        "type": entity_type,
                        "confidence": round(random.uniform(0.7, 0.99), 2),
                        "start": text_lower.find(word),
                        "end": text_lower.find(word) + len(word)
                    })
        
        return extracted


kg_store = KnowledgeGraphStore()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/entities")
async def create_entity(
    entity_type: EntityType = Query(...),
    name: str = Query(...),
    properties: dict = None,
    source: Optional[str] = Query(default=None)
):
    """Create an entity in the knowledge graph"""
    entity = kg_store.create_entity(entity_type.value, name, properties, source)
    return {"success": True, "entity": entity}


@router.get("/entities")
async def list_entities(
    query: Optional[str] = Query(default=None),
    entity_type: Optional[str] = Query(default=None),
    limit: int = Query(default=50, le=200)
):
    """Search entities"""
    return {"entities": kg_store.search_entities(query, entity_type, limit)}


@router.get("/entities/{entity_id}")
async def get_entity(entity_id: str):
    """Get entity details"""
    entity = kg_store.get_entity(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")
    return entity


@router.get("/entities/{entity_id}/neighbors")
async def get_entity_neighbors(entity_id: str, depth: int = Query(default=1, le=3)):
    """Get entity neighbors"""
    result = kg_store.get_neighbors(entity_id, depth)
    if not result:
        raise HTTPException(status_code=404, detail="Entity not found")
    return result


@router.post("/relationships")
async def create_relationship(
    source_id: str = Query(...),
    target_id: str = Query(...),
    relation_type: RelationType = Query(...),
    properties: dict = None
):
    """Create a relationship between entities"""
    try:
        rel = kg_store.create_relationship(
            source_id, target_id, relation_type.value, properties
        )
        return {"success": True, "relationship": rel}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/relationships")
async def list_relationships(
    entity_id: Optional[str] = Query(default=None),
    relation_type: Optional[str] = Query(default=None)
):
    """List relationships"""
    return {"relationships": kg_store.get_relationships(entity_id, relation_type)}


@router.post("/semantic-search")
async def semantic_search(
    query: str = Query(...),
    limit: int = Query(default=10, le=50)
):
    """Semantic similarity search"""
    return {"results": kg_store.semantic_search(query, limit)}


@router.post("/extract")
async def extract_entities(text: str = Query(...)):
    """Extract entities from text"""
    return {"entities": kg_store.extract_entities_from_text(text)}


@router.get("/ontology")
async def get_ontology():
    """Get ontology schema"""
    return {"ontology": kg_store.ontology}


@router.post("/ontology/{entity_type}")
async def update_ontology(
    entity_type: str,
    properties: Optional[List[str]] = Query(default=None),
    relationships: Optional[List[str]] = Query(default=None)
):
    """Update ontology for entity type"""
    if entity_type not in kg_store.ontology:
        kg_store.ontology[entity_type] = {"properties": [], "relationships": []}
    
    if properties:
        kg_store.ontology[entity_type]["properties"] = properties
    if relationships:
        kg_store.ontology[entity_type]["relationships"] = relationships
    
    return {"success": True, "ontology": kg_store.ontology[entity_type]}


@router.get("/graph/stats")
async def get_graph_stats():
    """Get knowledge graph statistics"""
    entity_counts = {}
    for e in kg_store.entities.values():
        etype = e.get("type", "unknown")
        entity_counts[etype] = entity_counts.get(etype, 0) + 1
    
    rel_counts = {}
    for r in kg_store.relationships:
        rtype = r.get("type", "unknown")
        rel_counts[rtype] = rel_counts.get(rtype, 0) + 1
    
    return {
        "total_entities": len(kg_store.entities),
        "total_relationships": len(kg_store.relationships),
        "entities_by_type": entity_counts,
        "relationships_by_type": rel_counts
    }


@router.get("/graph/visualize")
async def get_visualization_data(limit: int = Query(default=100)):
    """Get data for graph visualization"""
    entities = list(kg_store.entities.values())[:limit]
    entity_ids = {e["id"] for e in entities}
    
    relationships = [
        r for r in kg_store.relationships
        if r["source_id"] in entity_ids and r["target_id"] in entity_ids
    ]
    
    nodes = [
        {
            "id": e["id"],
            "label": e["name"],
            "type": e["type"],
            "size": 10
        }
        for e in entities
    ]
    
    edges = [
        {
            "id": r["id"],
            "source": r["source_id"],
            "target": r["target_id"],
            "label": r["type"]
        }
        for r in relationships
    ]
    
    return {"nodes": nodes, "edges": edges}
