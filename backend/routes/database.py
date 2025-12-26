"""
Database Configuration Routes
API endpoints for managing database connections
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Dict, List
import os

router = APIRouter()

# Store database configurations
database_configs: Dict[str, Dict] = {}


class PostgreSQLConfig(BaseModel):
    host: str
    port: int = 5432
    database: str
    user: str
    password: str


class DatabaseConfigRequest(BaseModel):
    db_type: str = "sqlite"  # sqlite or postgresql
    postgresql: Optional[PostgreSQLConfig] = None


@router.post("/database/config")
async def configure_database(config: DatabaseConfigRequest, job_id: str = Query(...)):
    """Configure database backend for a job"""
    
    if config.db_type not in ['sqlite', 'postgresql']:
        raise HTTPException(status_code=400, detail="Invalid db_type. Use: sqlite, postgresql")
    
    if config.db_type == 'postgresql':
        if not config.postgresql:
            raise HTTPException(status_code=400, detail="PostgreSQL config required")
        
        # Test connection
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=config.postgresql.host,
                port=config.postgresql.port,
                database=config.postgresql.database,
                user=config.postgresql.user,
                password=config.postgresql.password
            )
            conn.close()
        except ImportError:
            raise HTTPException(
                status_code=500, 
                detail="PostgreSQL support requires psycopg2: pip install psycopg2-binary"
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")
    
    # Store config
    database_configs[job_id] = {
        'db_type': config.db_type,
        'postgresql': config.postgresql.dict() if config.postgresql else None
    }
    
    return {
        "message": f"Database configured: {config.db_type}",
        "job_id": job_id
    }


@router.get("/database/config/{job_id}")
async def get_database_config(job_id: str):
    """Get database configuration for a job"""
    
    config = database_configs.get(job_id, {'db_type': 'sqlite'})
    
    # Don't expose password
    if config.get('postgresql'):
        config = dict(config)
        config['postgresql'] = dict(config['postgresql'])
        config['postgresql']['password'] = '***'
    
    return {"config": config}


@router.get("/database/test")
async def test_postgresql_connection(
    host: str = Query(...),
    port: int = Query(5432),
    database: str = Query(...),
    user: str = Query(...),
    password: str = Query(...)
):
    """Test PostgreSQL connection"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        # Get server version
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        
        # Get list of tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "message": "Connection successful",
            "version": version,
            "tables": tables
        }
        
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="PostgreSQL support requires psycopg2: pip install psycopg2-binary"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")


@router.get("/database/types")
async def get_supported_databases():
    """Get list of supported database types"""
    return {
        "databases": [
            {
                "type": "sqlite",
                "name": "SQLite",
                "description": "File-based database, good for development and small datasets",
                "requires": []
            },
            {
                "type": "postgresql",
                "name": "PostgreSQL",
                "description": "Production-grade database, supports large datasets and concurrent access",
                "requires": ["psycopg2-binary"]
            }
        ]
    }
