"""
Database Converter Service
Supports multiple database backends: SQLite, PostgreSQL
"""

import os
import sqlite3
from typing import Dict, List, Optional, Any, Generator
from dataclasses import dataclass
from abc import ABC, abstractmethod
from contextlib import contextmanager


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    db_type: str  # sqlite, postgresql
    # SQLite
    file_path: Optional[str] = None
    # PostgreSQL
    host: Optional[str] = None
    port: Optional[int] = 5432
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    
    def get_connection_string(self) -> str:
        """Get connection string for the database"""
        if self.db_type == 'sqlite':
            return self.file_path or ':memory:'
        elif self.db_type == 'postgresql':
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return ''


class DatabaseBackend(ABC):
    """Abstract base class for database backends"""
    
    @abstractmethod
    def connect(self):
        """Connect to the database"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Disconnect from the database"""
        pass
    
    @abstractmethod
    def create_table(self, table_name: str, columns: List[Dict]):
        """Create a table with the given schema"""
        pass
    
    @abstractmethod
    def insert_batch(self, table_name: str, records: List[Dict]):
        """Insert a batch of records"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a query and return results"""
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get schema for a table"""
        pass


class SQLiteBackend(DatabaseBackend):
    """SQLite database backend"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
    
    def connect(self):
        os.makedirs(os.path.dirname(self.config.file_path) or '.', exist_ok=True)
        self.connection = sqlite3.connect(self.config.file_path)
        self.connection.row_factory = sqlite3.Row
    
    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None
    
    @contextmanager
    def get_cursor(self):
        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
    
    def create_table(self, table_name: str, columns: List[Dict]):
        col_defs = []
        for col in columns:
            name = col['name'].replace('"', '""')
            sql_type = col.get('sql_type', 'TEXT')
            col_defs.append(f'"{name}" {sql_type}')
        
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)})'
        
        with self.get_cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            cursor.execute(create_sql)
    
    def insert_batch(self, table_name: str, records: List[Dict]):
        if not records:
            return
        
        columns = list(records[0].keys())
        placeholders = ', '.join(['?' for _ in columns])
        col_names = ', '.join([f'"{c}"' for c in columns])
        
        insert_sql = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})'
        
        with self.get_cursor() as cursor:
            for record in records:
                values = [record.get(col) for col in columns]
                cursor.execute(insert_sql, values)
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        with self.get_cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            return []
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            rows = cursor.fetchall()
            return [
                {'name': row[1], 'type': row[2], 'sql_type': row[2]}
                for row in rows
            ]


class PostgreSQLBackend(DatabaseBackend):
    """PostgreSQL database backend"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
    
    def connect(self):
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password
            )
            self._dict_cursor = RealDictCursor
        except ImportError:
            raise ImportError("PostgreSQL support requires psycopg2: pip install psycopg2-binary")
    
    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None
    
    @contextmanager
    def get_cursor(self, dict_cursor=False):
        cursor_factory = self._dict_cursor if dict_cursor else None
        cursor = self.connection.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
    
    def _map_sql_type(self, sql_type: str) -> str:
        """Map SQLite types to PostgreSQL types"""
        type_map = {
            'TEXT': 'TEXT',
            'INTEGER': 'BIGINT',
            'REAL': 'DOUBLE PRECISION',
            'BLOB': 'BYTEA',
            'NUMERIC': 'NUMERIC',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP'
        }
        return type_map.get(sql_type.upper(), 'TEXT')
    
    def create_table(self, table_name: str, columns: List[Dict]):
        col_defs = []
        for col in columns:
            name = col['name'].replace('"', '""')
            sql_type = self._map_sql_type(col.get('sql_type', 'TEXT'))
            col_defs.append(f'"{name}" {sql_type}')
        
        with self.get_cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            create_sql = f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})'
            cursor.execute(create_sql)
    
    def insert_batch(self, table_name: str, records: List[Dict]):
        if not records:
            return
        
        columns = list(records[0].keys())
        placeholders = ', '.join(['%s' for _ in columns])
        col_names = ', '.join([f'"{c}"' for c in columns])
        
        insert_sql = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})'
        
        with self.get_cursor() as cursor:
            for record in records:
                values = [record.get(col) for col in columns]
                cursor.execute(insert_sql, values)
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        # Convert SQLite-style ? placeholders to PostgreSQL %s
        pg_query = query.replace('?', '%s')
        
        with self.get_cursor(dict_cursor=True) as cursor:
            if params:
                cursor.execute(pg_query, params)
            else:
                cursor.execute(pg_query)
            
            if cursor.description:
                return [dict(row) for row in cursor.fetchall()]
            return []
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        with self.get_cursor(dict_cursor=True) as cursor:
            cursor.execute(query, (table_name,))
            return [
                {'name': row['column_name'], 'type': row['data_type'], 'sql_type': row['data_type']}
                for row in cursor.fetchall()
            ]


def create_backend(config: DatabaseConfig) -> DatabaseBackend:
    """Factory function to create appropriate database backend"""
    if config.db_type == 'sqlite':
        return SQLiteBackend(config)
    elif config.db_type == 'postgresql':
        return PostgreSQLBackend(config)
    else:
        raise ValueError(f"Unsupported database type: {config.db_type}")


class UniversalConverter:
    """Convert data from any parser to any database backend"""
    
    def __init__(self, backend: DatabaseBackend, table_name: str = "converted_data"):
        self.backend = backend
        self.table_name = table_name
    
    def convert(
        self,
        parser,
        batch_size: int = 10000,
        progress_callback=None
    ) -> Dict:
        """Convert data from parser to database"""
        
        # Connect to database
        self.backend.connect()
        
        try:
            # Get schema and create table
            schema = parser.get_schema()
            
            # Convert schema to dict format if needed
            if hasattr(schema[0], 'name'):
                schema_list = [
                    {'name': col.name, 'type': col.data_type, 'sql_type': col.sql_type}
                    for col in schema
                ]
            else:
                schema_list = schema
            
            self.backend.create_table(self.table_name, schema_list)
            
            # Get estimated row count
            try:
                total_records = parser.get_row_count()
            except:
                total_records = 0
            
            # Process records in batches
            processed = 0
            
            for batch in parser.read_records(batch_size):
                self.backend.insert_batch(self.table_name, batch)
                processed += len(batch)
                
                if progress_callback:
                    percentage = (processed / total_records * 100) if total_records > 0 else 0
                    progress_callback({
                        'status': 'processing',
                        'processed_records': processed,
                        'total_records': total_records,
                        'percentage': min(percentage, 99),
                        'message': f'Processed {processed:,} records...'
                    })
            
            # Complete
            if progress_callback:
                progress_callback({
                    'status': 'completed',
                    'processed_records': processed,
                    'total_records': processed,
                    'percentage': 100,
                    'message': f'Completed! {processed:,} records imported.'
                })
            
            return {
                'success': True,
                'records': processed,
                'schema': schema_list
            }
            
        except Exception as e:
            if progress_callback:
                progress_callback({
                    'status': 'error',
                    'message': str(e),
                    'error': str(e)
                })
            raise
        
        finally:
            self.backend.disconnect()
