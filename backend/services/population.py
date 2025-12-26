"""
Population Management Service
Create, manage, and analyze data segments
"""

import sqlite3
import json
import uuid
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum


class CombineOperation(str, Enum):
    UNION = "union"
    INTERSECT = "intersect"
    EXCLUDE = "exclude"


@dataclass
class Population:
    """Represents a saved data population/segment"""
    id: str
    name: str
    description: str
    job_id: str
    query: str  # The SQL query that defines this population
    natural_language: str  # Original NL query
    filters: Dict = field(default_factory=dict)
    count: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    tags: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return asdict(self)


class PopulationManager:
    """Manage data populations for a job"""
    
    def __init__(self, job_id: str, db_path: str, table_name: str = "converted_data"):
        self.job_id = job_id
        self.db_path = db_path
        self.table_name = table_name
        self.populations: Dict[str, Population] = {}
        self._storage_file = f"databases/{job_id}_populations.json"
        self._load_populations()
    
    def _load_populations(self):
        """Load saved populations from storage"""
        import os
        if os.path.exists(self._storage_file):
            try:
                with open(self._storage_file, 'r') as f:
                    data = json.load(f)
                    for pop_data in data.get('populations', []):
                        pop = Population(**pop_data)
                        self.populations[pop.id] = pop
            except Exception as e:
                print(f"Error loading populations: {e}")
    
    def _save_populations(self):
        """Save populations to storage"""
        import os
        os.makedirs("databases", exist_ok=True)
        with open(self._storage_file, 'w') as f:
            json.dump({
                'populations': [p.to_dict() for p in self.populations.values()]
            }, f, indent=2)
    
    def create_population(
        self,
        name: str,
        query: str,
        natural_language: str = "",
        description: str = "",
        tags: List[str] = None
    ) -> Population:
        """Create a new population from a query"""
        pop_id = str(uuid.uuid4())[:12]
        
        # Get count by running the query
        count = self._get_query_count(query)
        
        population = Population(
            id=pop_id,
            name=name,
            description=description,
            job_id=self.job_id,
            query=query,
            natural_language=natural_language,
            count=count,
            tags=tags or []
        )
        
        self.populations[pop_id] = population
        self._save_populations()
        
        return population
    
    def _get_query_count(self, query: str) -> int:
        """Get count of records matching a query"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Wrap query in COUNT
            count_query = f"SELECT COUNT(*) FROM ({query})"
            cursor.execute(count_query)
            count = cursor.fetchone()[0]
            
            conn.close()
            return count
        except Exception as e:
            print(f"Error counting: {e}")
            return 0
    
    def get_population(self, pop_id: str) -> Optional[Population]:
        """Get a population by ID"""
        return self.populations.get(pop_id)
    
    def list_populations(self) -> List[Population]:
        """List all populations for this job"""
        return list(self.populations.values())
    
    def delete_population(self, pop_id: str) -> bool:
        """Delete a population"""
        if pop_id in self.populations:
            del self.populations[pop_id]
            self._save_populations()
            return True
        return False
    
    def refresh_population(self, pop_id: str) -> Optional[Population]:
        """Refresh population count by re-running query"""
        pop = self.populations.get(pop_id)
        if not pop:
            return None
        
        pop.count = self._get_query_count(pop.query)
        pop.updated_at = datetime.now().isoformat()
        self._save_populations()
        
        return pop
    
    def get_population_data(
        self, 
        pop_id: str, 
        limit: int = 1000,
        offset: int = 0
    ) -> Dict:
        """Get actual records for a population"""
        pop = self.populations.get(pop_id)
        if not pop:
            return {"error": "Population not found"}
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Add limit/offset to query
            query = pop.query.rstrip(';')
            if 'LIMIT' not in query.upper():
                query = f"{query} LIMIT {limit} OFFSET {offset}"
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            columns = [desc[0] for desc in cursor.description]
            records = [dict(row) for row in rows]
            
            conn.close()
            
            return {
                "population": pop.to_dict(),
                "columns": columns,
                "records": records,
                "count": len(records),
                "total": pop.count
            }
        except Exception as e:
            return {"error": str(e)}
    
    def combine_populations(
        self,
        pop_ids: List[str],
        operation: CombineOperation,
        new_name: str
    ) -> Optional[Population]:
        """Combine multiple populations using set operations"""
        pops = [self.populations.get(pid) for pid in pop_ids]
        if not all(pops):
            return None
        
        queries = [p.query.rstrip(';') for p in pops]
        
        if operation == CombineOperation.UNION:
            combined_query = " UNION ".join(f"({q})" for q in queries)
        elif operation == CombineOperation.INTERSECT:
            combined_query = " INTERSECT ".join(f"({q})" for q in queries)
        elif operation == CombineOperation.EXCLUDE:
            # First population minus others
            if len(queries) < 2:
                return None
            combined_query = f"({queries[0]}) EXCEPT " + " EXCEPT ".join(f"({q})" for q in queries[1:])
        else:
            return None
        
        # Create new population from combined query
        return self.create_population(
            name=new_name,
            query=combined_query,
            natural_language=f"Combined: {operation.value} of {', '.join(p.name for p in pops)}",
            description=f"Created by {operation.value} operation",
            tags=["combined"]
        )
    
    def get_population_stats(self, pop_id: str) -> Dict:
        """Get statistical summary of a population"""
        pop = self.populations.get(pop_id)
        if not pop:
            return {"error": "Population not found"}
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get schema
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns = cursor.fetchall()
            
            stats = {
                "population": pop.to_dict(),
                "columns": {},
                "total_records": pop.count
            }
            
            # Get stats for each column
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                
                col_stats = {"name": col_name, "type": col_type}
                
                # For numeric columns
                if col_type in ['INTEGER', 'REAL']:
                    query = f"""
                        SELECT 
                            MIN("{col_name}") as min_val,
                            MAX("{col_name}") as max_val,
                            AVG("{col_name}") as avg_val,
                            SUM("{col_name}") as sum_val
                        FROM ({pop.query})
                    """
                    cursor.execute(query)
                    row = cursor.fetchone()
                    col_stats.update({
                        "min": row[0],
                        "max": row[1],
                        "avg": row[2],
                        "sum": row[3]
                    })
                
                # For all columns - null count and unique count
                null_query = f'SELECT COUNT(*) FROM ({pop.query}) WHERE "{col_name}" IS NULL'
                cursor.execute(null_query)
                col_stats["null_count"] = cursor.fetchone()[0]
                
                unique_query = f'SELECT COUNT(DISTINCT "{col_name}") FROM ({pop.query})'
                cursor.execute(unique_query)
                col_stats["unique_count"] = cursor.fetchone()[0]
                
                stats["columns"][col_name] = col_stats
            
            conn.close()
            return stats
            
        except Exception as e:
            return {"error": str(e)}


# Global storage for population managers per job
_population_managers: Dict[str, PopulationManager] = {}


def get_population_manager(job_id: str, db_path: str) -> PopulationManager:
    """Get or create a population manager for a job"""
    if job_id not in _population_managers:
        _population_managers[job_id] = PopulationManager(job_id, db_path)
    return _population_managers[job_id]
