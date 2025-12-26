"""
NLP Query Service
Handles natural language to SQL conversion using Gemini API
"""

import os
import sqlite3
import json
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import re


@dataclass
class QueryResult:
    """Result of an NLP query"""
    query_id: str
    natural_language: str
    sql_query: str
    results: List[Dict[str, Any]]
    columns: List[str]
    row_count: int
    execution_time_ms: float
    timestamp: str
    error: Optional[str] = None


class NlpQueryService:
    """
    Service for converting natural language to SQL and executing queries
    Uses Gemini API for NL to SQL conversion
    """

    def __init__(self, db_path: str, table_name: str = "data"):
        """
        Initialize the query service
        
        Args:
            db_path: Path to the SQLite database
            table_name: Name of the main data table
        """
        self.db_path = db_path
        self.table_name = table_name
        self.schema_context: str = ""
        self.query_history: List[QueryResult] = []
        self._load_schema()
        self._init_gemini()

    def _init_gemini(self):
        """Initialize Gemini API client"""
        self.gemini_model = None
        api_key = os.getenv("GEMINI_API_KEY")
        
        if api_key:
            try:
                import google.generativeai as genai
                genai.configure(api_key=api_key)
                self.gemini_model = genai.GenerativeModel('gemini-2.0-flash')
            except Exception as e:
                print(f"Warning: Could not initialize Gemini: {e}")

    def _load_schema(self):
        """Load database schema for context"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get table info
            cursor.execute(f'PRAGMA table_info("{self.table_name}")')
            columns = cursor.fetchall()
            
            # Build schema context
            schema_parts = [f"Table: {self.table_name}"]
            schema_parts.append("Columns:")
            for col in columns:
                col_name, col_type = col[1], col[2]
                schema_parts.append(f"  - {col_name} ({col_type})")
            
            # Get sample data for context
            cursor.execute(f'SELECT * FROM "{self.table_name}" LIMIT 3')
            sample_rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            
            if sample_rows:
                schema_parts.append("\nSample data:")
                for row in sample_rows:
                    sample = dict(zip(col_names, row))
                    schema_parts.append(f"  {json.dumps(sample, default=str)}")
            
            conn.close()
            self.schema_context = "\n".join(schema_parts)
            
        except Exception as e:
            self.schema_context = f"Error loading schema: {str(e)}"

    def _generate_sql_with_gemini(self, natural_language: str) -> str:
        """
        Use Gemini to convert natural language to SQL
        
        Args:
            natural_language: The natural language query
            
        Returns:
            SQL query string
        """
        if not self.gemini_model:
            return self._fallback_sql_generation(natural_language)
        
        prompt = f"""You are a SQL expert. Convert the following natural language query to a SQLite SQL query.

Database Schema:
{self.schema_context}

Rules:
1. Only generate SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
2. Use double quotes for column names with spaces or special characters
3. Always include a LIMIT clause (max 1000 rows) unless the user specifies otherwise
4. If the query is ambiguous, make reasonable assumptions
5. Return ONLY the SQL query, no explanations

Natural language query: {natural_language}

SQL query:"""

        try:
            response = self.gemini_model.generate_content(prompt)
            sql = response.text.strip()
            
            # Clean up the response
            sql = self._clean_sql_response(sql)
            
            # Validate it's a SELECT query
            if not self._is_safe_query(sql):
                raise ValueError("Only SELECT queries are allowed")
            
            return sql
            
        except Exception as e:
            print(f"Gemini error: {e}")
            return self._fallback_sql_generation(natural_language)

    def _clean_sql_response(self, sql: str) -> str:
        """Clean up SQL response from AI"""
        # Remove markdown code blocks
        sql = re.sub(r'```sql\s*', '', sql)
        sql = re.sub(r'```\s*', '', sql)
        sql = sql.strip()
        
        # Remove any trailing semicolons and add one back
        sql = sql.rstrip(';') + ';'
        
        return sql

    def _fallback_sql_generation(self, natural_language: str) -> str:
        """
        Simple fallback SQL generation without AI
        Handles basic queries
        """
        nl_lower = natural_language.lower()
        
        # Count queries
        if 'count' in nl_lower or 'how many' in nl_lower:
            return f'SELECT COUNT(*) as count FROM "{self.table_name}";'
        
        # All records
        if 'all' in nl_lower and ('show' in nl_lower or 'get' in nl_lower or 'list' in nl_lower):
            return f'SELECT * FROM "{self.table_name}" LIMIT 100;'
        
        # Look for comparisons
        comparison_match = re.search(
            r'where\s+(\w+)\s*(>|<|>=|<=|=|!=)\s*(\d+(?:\.\d+)?)',
            nl_lower
        )
        if comparison_match:
            col, op, val = comparison_match.groups()
            return f'SELECT * FROM "{self.table_name}" WHERE "{col}" {op} {val} LIMIT 100;'
        
        # Look for specific column mentions
        column_match = re.search(r'(show|get|select)\s+(\w+)', nl_lower)
        if column_match:
            col = column_match.group(2)
            return f'SELECT "{col}" FROM "{self.table_name}" LIMIT 100;'
        
        # Default: return all with limit
        return f'SELECT * FROM "{self.table_name}" LIMIT 100;'

    def _is_safe_query(self, sql: str) -> bool:
        """Check if the query is safe (SELECT only)"""
        sql_upper = sql.upper().strip()
        
        # Must start with SELECT or WITH (for CTEs)
        if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
            return False
        
        # Block dangerous keywords
        dangerous = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 
                     'CREATE', 'TRUNCATE', 'EXEC', 'EXECUTE']
        for keyword in dangerous:
            if re.search(rf'\b{keyword}\b', sql_upper):
                return False
        
        return True

    def execute_query(self, sql: str, max_rows: int = 1000) -> Tuple[List[Dict], List[str], int]:
        """
        Execute a SQL query and return results
        
        Args:
            sql: SQL query to execute
            max_rows: Maximum rows to return
            
        Returns:
            Tuple of (results, column_names, total_count)
        """
        if not self._is_safe_query(sql):
            raise ValueError("Only SELECT queries are allowed")
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql)
            rows = cursor.fetchmany(max_rows)
            
            if rows:
                columns = list(rows[0].keys())
                results = [dict(row) for row in rows]
            else:
                columns = []
                results = []
            
            # Get total count if there's a LIMIT
            total = len(results)
            
            conn.close()
            return results, columns, total
            
        except Exception as e:
            conn.close()
            raise e

    def process_natural_language_query(
        self,
        natural_language: str,
        max_rows: int = 1000
    ) -> QueryResult:
        """
        Process a natural language query end-to-end
        
        Args:
            natural_language: The natural language query
            max_rows: Maximum rows to return
            
        Returns:
            QueryResult with SQL and results
        """
        import time
        import uuid
        
        query_id = str(uuid.uuid4())[:8]
        start_time = time.time()
        
        try:
            # Generate SQL from natural language
            sql = self._generate_sql_with_gemini(natural_language)
            
            # Execute the query
            results, columns, count = self.execute_query(sql, max_rows)
            
            execution_time = (time.time() - start_time) * 1000
            
            result = QueryResult(
                query_id=query_id,
                natural_language=natural_language,
                sql_query=sql,
                results=results,
                columns=columns,
                row_count=count,
                execution_time_ms=round(execution_time, 2),
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            result = QueryResult(
                query_id=query_id,
                natural_language=natural_language,
                sql_query="",
                results=[],
                columns=[],
                row_count=0,
                execution_time_ms=round(execution_time, 2),
                timestamp=datetime.now().isoformat(),
                error=str(e)
            )
        
        # Add to history
        self.query_history.append(result)
        
        # Keep only last 100 queries
        if len(self.query_history) > 100:
            self.query_history = self.query_history[-100:]
        
        return result

    def get_history(self, limit: int = 20) -> List[Dict]:
        """Get query history"""
        history = self.query_history[-limit:]
        return [
            {
                "query_id": q.query_id,
                "natural_language": q.natural_language,
                "sql_query": q.sql_query,
                "row_count": q.row_count,
                "execution_time_ms": q.execution_time_ms,
                "timestamp": q.timestamp,
                "error": q.error
            }
            for q in reversed(history)
        ]

    def export_results(self, query_result: QueryResult, format: str = 'json') -> str:
        """
        Export query results to specified format
        
        Args:
            query_result: The query result to export
            format: 'json' or 'csv'
            
        Returns:
            Formatted string
        """
        if format == 'csv':
            import csv
            import io
            
            output = io.StringIO()
            if query_result.results:
                writer = csv.DictWriter(output, fieldnames=query_result.columns)
                writer.writeheader()
                writer.writerows(query_result.results)
            return output.getvalue()
        
        else:  # json
            return json.dumps({
                "query": query_result.natural_language,
                "sql": query_result.sql_query,
                "columns": query_result.columns,
                "data": query_result.results,
                "row_count": query_result.row_count
            }, indent=2, default=str)
