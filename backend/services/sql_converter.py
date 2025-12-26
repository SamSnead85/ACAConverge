"""
SQL Converter Service
Handles conversion of YXDB data to SQLite database
"""

import sqlite3
import os
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class ConversionProgress:
    """Track conversion progress"""
    status: str  # 'pending', 'processing', 'completed', 'error'
    total_records: int
    processed_records: int
    percentage: float
    message: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None


class SqlConverter:
    """
    Converts YXDB data to SQLite database
    Supports streaming inserts with progress tracking
    """

    def __init__(self, db_path: str, table_name: str = "data"):
        """
        Initialize the converter
        
        Args:
            db_path: Path to create the SQLite database
            table_name: Name of the table to create
        """
        self.db_path = db_path
        self.table_name = self._sanitize_name(table_name)
        self.connection: Optional[sqlite3.Connection] = None
        self.schema: List[Dict[str, Any]] = []
        self.progress = ConversionProgress(
            status='pending',
            total_records=0,
            processed_records=0,
            percentage=0.0,
            message='Waiting to start'
        )

    def _sanitize_name(self, name: str) -> str:
        """Sanitize table/column names for SQL"""
        # Replace spaces and special characters
        sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = '_' + sanitized
        return sanitized or 'data'

    def _connect(self):
        """Create database connection"""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA temp_store=MEMORY")
        self.connection.execute("PRAGMA cache_size=-64000")  # 64MB cache

    def _close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def create_table(self, schema: List[Dict[str, Any]]) -> str:
        """
        Create table based on YXDB schema
        
        Args:
            schema: List of field definitions from YxdbParser
            
        Returns:
            CREATE TABLE SQL statement
        """
        self.schema = schema
        columns = []
        
        for field in schema:
            col_name = self._sanitize_name(field['name'])
            sql_type = field.get('sql_type', 'TEXT')
            columns.append(f'"{col_name}" {sql_type}')
        
        # Add metadata columns
        columns.append('"_row_id" INTEGER PRIMARY KEY AUTOINCREMENT')
        columns.append('"_imported_at" TEXT')
        
        create_sql = f'CREATE TABLE IF NOT EXISTS "{self.table_name}" ({", ".join(columns)})'
        
        self._connect()
        self.connection.execute(create_sql)
        self.connection.commit()
        
        return create_sql

    def insert_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert a batch of records
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Number of records inserted
        """
        if not records or not self.connection:
            return 0
        
        # Get column names from schema
        columns = [self._sanitize_name(f['name']) for f in self.schema]
        columns.append('_imported_at')
        
        placeholders = ', '.join(['?' for _ in columns])
        column_names = ', '.join([f'"{c}"' for c in columns])
        
        insert_sql = f'INSERT INTO "{self.table_name}" ({column_names}) VALUES ({placeholders})'
        
        now = datetime.now().isoformat()
        rows = []
        
        for record in records:
            row = []
            for field in self.schema:
                value = record.get(field['name'])
                # Handle special types
                if value is not None:
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    elif isinstance(value, bytes):
                        value = value.hex()
                row.append(value)
            row.append(now)
            rows.append(tuple(row))
        
        self.connection.executemany(insert_sql, rows)
        self.connection.commit()
        
        return len(records)

    def convert_from_parser(
        self,
        parser,
        batch_size: int = 10000,
        progress_callback: Optional[Callable[[ConversionProgress], None]] = None,
        estimated_records: int = 0
    ) -> ConversionProgress:
        """
        Convert data from a YXDB parser to SQLite
        
        Args:
            parser: YxdbParser or MockYxdbParser instance
            batch_size: Records per batch
            progress_callback: Optional callback for progress updates
            estimated_records: Estimated total records for progress calculation
            
        Returns:
            Final progress state
        """
        self.progress = ConversionProgress(
            status='processing',
            total_records=estimated_records,
            processed_records=0,
            percentage=0.0,
            message='Creating table schema...',
            started_at=datetime.now().isoformat()
        )
        
        if progress_callback:
            progress_callback(self.progress)
        
        try:
            # Create table from schema - handle both ColumnInfo and dict formats
            if hasattr(parser, 'get_schema_as_dicts'):
                schema = parser.get_schema_as_dicts()
            else:
                schema = parser.get_schema()
                # Convert ColumnInfo to dict if needed
                if schema and hasattr(schema[0], 'to_dict'):
                    schema = [col.to_dict() for col in schema]
            self.create_table(schema)
            
            self.progress.message = 'Inserting records...'
            if progress_callback:
                progress_callback(self.progress)
            
            # Stream records in batches
            for batch in parser.stream_records(batch_size):
                inserted = self.insert_batch(batch)
                self.progress.processed_records += inserted
                
                if estimated_records > 0:
                    self.progress.percentage = min(
                        (self.progress.processed_records / estimated_records) * 100,
                        99.9
                    )
                
                self.progress.message = f'Processed {self.progress.processed_records:,} records...'
                
                if progress_callback:
                    progress_callback(self.progress)
            
            # Create indexes for common query patterns
            self._create_indexes()
            
            self.progress.status = 'completed'
            self.progress.percentage = 100.0
            self.progress.total_records = self.progress.processed_records
            self.progress.message = f'Successfully converted {self.progress.processed_records:,} records'
            self.progress.completed_at = datetime.now().isoformat()
            
        except Exception as e:
            self.progress.status = 'error'
            self.progress.error = str(e)
            self.progress.message = f'Error: {str(e)}'
        
        finally:
            self._close()
            parser.close()
        
        if progress_callback:
            progress_callback(self.progress)
        
        return self.progress

    def _create_indexes(self):
        """Create indexes on commonly queried columns"""
        if not self.connection:
            return
        
        # Create index on first few text/numeric columns for faster queries
        indexed = 0
        for field in self.schema[:5]:  # Index first 5 columns
            if field['sql_type'] in ('TEXT', 'INTEGER', 'REAL'):
                col_name = self._sanitize_name(field['name'])
                try:
                    self.connection.execute(
                        f'CREATE INDEX IF NOT EXISTS "idx_{col_name}" ON "{self.table_name}" ("{col_name}")'
                    )
                    indexed += 1
                except:
                    pass
        
        self.connection.commit()

    def get_db_size(self) -> int:
        """Get size of the database file in bytes"""
        if os.path.exists(self.db_path):
            return os.path.getsize(self.db_path)
        return 0

    def get_table_info(self) -> Dict[str, Any]:
        """Get information about the created table"""
        self._connect()
        cursor = self.connection.cursor()
        
        # Get column info
        cursor.execute(f'PRAGMA table_info("{self.table_name}")')
        columns = [
            {"name": row[1], "type": row[2], "nullable": not row[3]}
            for row in cursor.fetchall()
        ]
        
        # Get row count
        cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
        row_count = cursor.fetchone()[0]
        
        self._close()
        
        return {
            "table_name": self.table_name,
            "columns": columns,
            "row_count": row_count,
            "db_size_bytes": self.get_db_size()
        }
