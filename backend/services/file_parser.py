"""
Multi-Format File Parser
Supports: YXDB, Excel (.xlsx, .xls), CSV, JSON, Parquet
"""

import os
import csv
import json
import sqlite3
from typing import Dict, List, Generator, Optional, Any, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass
class ColumnInfo:
    """Column metadata"""
    name: str
    data_type: str
    sql_type: str
    sample_values: List[Any] = None


class BaseParser(ABC):
    """Base class for file parsers"""
    
    @abstractmethod
    def get_schema(self) -> List[ColumnInfo]:
        """Get column schema"""
        pass
    
    @abstractmethod
    def read_records(self, chunk_size: int = 10000) -> Generator[List[Dict], None, None]:
        """Read records in chunks"""
        pass
    
    @abstractmethod
    def get_row_count(self) -> int:
        """Get total row count (estimate for large files)"""
        pass


class CSVParser(BaseParser):
    """Parse CSV files"""
    
    def __init__(self, file_path: str, delimiter: str = ',', encoding: str = 'utf-8'):
        self.file_path = file_path
        self.delimiter = delimiter
        self.encoding = encoding
        self._schema = None
        self._row_count = None
    
    def get_schema(self) -> List[ColumnInfo]:
        if self._schema:
            return self._schema
        
        with open(self.file_path, 'r', encoding=self.encoding, errors='replace') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            headers = reader.fieldnames or []
            
            # Sample first 100 rows to infer types
            samples = {h: [] for h in headers}
            for i, row in enumerate(reader):
                if i >= 100:
                    break
                for h in headers:
                    if row.get(h):
                        samples[h].append(row[h])
            
            self._schema = []
            for header in headers:
                data_type, sql_type = self._infer_type(samples.get(header, []))
                self._schema.append(ColumnInfo(
                    name=header,
                    data_type=data_type,
                    sql_type=sql_type,
                    sample_values=samples.get(header, [])[:5]
                ))
        
        return self._schema
    
    def _infer_type(self, samples: List[str]) -> Tuple[str, str]:
        """Infer column type from sample values"""
        if not samples:
            return 'text', 'TEXT'
        
        # Try integer
        try:
            for s in samples[:20]:
                if s:
                    int(s.replace(',', ''))
            return 'integer', 'INTEGER'
        except ValueError:
            pass
        
        # Try float
        try:
            for s in samples[:20]:
                if s:
                    float(s.replace(',', ''))
            return 'float', 'REAL'
        except ValueError:
            pass
        
        return 'text', 'TEXT'
    
    def read_records(self, chunk_size: int = 10000) -> Generator[List[Dict], None, None]:
        with open(self.file_path, 'r', encoding=self.encoding, errors='replace') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            chunk = []
            
            for row in reader:
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            
            if chunk:
                yield chunk
    
    def get_row_count(self) -> int:
        if self._row_count is None:
            with open(self.file_path, 'r', encoding=self.encoding, errors='replace') as f:
                self._row_count = sum(1 for _ in f) - 1  # Subtract header
        return max(0, self._row_count)


class ExcelParser(BaseParser):
    """Parse Excel files (.xlsx, .xls)"""
    
    def __init__(self, file_path: str, sheet_name: str = None):
        self.file_path = file_path
        self.sheet_name = sheet_name
        self._schema = None
        self._data = None
    
    def _load_data(self):
        """Load Excel data using openpyxl or xlrd"""
        if self._data is not None:
            return
        
        try:
            import openpyxl
            wb = openpyxl.load_workbook(self.file_path, read_only=True, data_only=True)
            sheet = wb[self.sheet_name] if self.sheet_name else wb.active
            
            rows = list(sheet.iter_rows(values_only=True))
            if not rows:
                self._data = {'headers': [], 'rows': []}
                return
            
            headers = [str(h) if h else f'Column_{i}' for i, h in enumerate(rows[0])]
            self._data = {
                'headers': headers,
                'rows': rows[1:]
            }
            wb.close()
        except ImportError:
            # Fallback: try pandas
            try:
                import pandas as pd
                df = pd.read_excel(self.file_path, sheet_name=self.sheet_name or 0)
                self._data = {
                    'headers': list(df.columns),
                    'rows': df.values.tolist()
                }
            except ImportError:
                raise ImportError("Install openpyxl or pandas for Excel support: pip install openpyxl")
    
    def get_schema(self) -> List[ColumnInfo]:
        if self._schema:
            return self._schema
        
        self._load_data()
        headers = self._data['headers']
        rows = self._data['rows'][:100]
        
        self._schema = []
        for i, header in enumerate(headers):
            samples = [row[i] for row in rows if len(row) > i and row[i] is not None]
            data_type, sql_type = self._infer_type(samples)
            self._schema.append(ColumnInfo(
                name=header,
                data_type=data_type,
                sql_type=sql_type,
                sample_values=samples[:5]
            ))
        
        return self._schema
    
    def _infer_type(self, samples: List[Any]) -> Tuple[str, str]:
        if not samples:
            return 'text', 'TEXT'
        
        sample = samples[0]
        if isinstance(sample, int):
            return 'integer', 'INTEGER'
        elif isinstance(sample, float):
            return 'float', 'REAL'
        elif isinstance(sample, bool):
            return 'boolean', 'INTEGER'
        return 'text', 'TEXT'
    
    def read_records(self, chunk_size: int = 10000) -> Generator[List[Dict], None, None]:
        self._load_data()
        headers = self._data['headers']
        rows = self._data['rows']
        
        chunk = []
        for row in rows:
            record = {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
            chunk.append(record)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        
        if chunk:
            yield chunk
    
    def get_row_count(self) -> int:
        self._load_data()
        return len(self._data['rows'])


class JSONParser(BaseParser):
    """Parse JSON files (array of objects or newline-delimited)"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._schema = None
        self._data = None
        self._is_ndjson = False
    
    def _load_data(self):
        if self._data is not None:
            return
        
        with open(self.file_path, 'r', encoding='utf-8') as f:
            first_char = f.read(1)
            f.seek(0)
            
            if first_char == '[':
                # Standard JSON array
                self._data = json.load(f)
            else:
                # Newline-delimited JSON
                self._is_ndjson = True
                self._data = [json.loads(line) for line in f if line.strip()]
    
    def get_schema(self) -> List[ColumnInfo]:
        if self._schema:
            return self._schema
        
        self._load_data()
        if not self._data:
            return []
        
        # Get all keys from first 100 records
        all_keys = set()
        for record in self._data[:100]:
            if isinstance(record, dict):
                all_keys.update(record.keys())
        
        self._schema = []
        for key in sorted(all_keys):
            samples = [r.get(key) for r in self._data[:100] if isinstance(r, dict) and r.get(key) is not None]
            data_type, sql_type = self._infer_type(samples)
            self._schema.append(ColumnInfo(
                name=key,
                data_type=data_type,
                sql_type=sql_type,
                sample_values=samples[:5]
            ))
        
        return self._schema
    
    def _infer_type(self, samples: List[Any]) -> Tuple[str, str]:
        if not samples:
            return 'text', 'TEXT'
        
        sample = samples[0]
        if isinstance(sample, int):
            return 'integer', 'INTEGER'
        elif isinstance(sample, float):
            return 'float', 'REAL'
        elif isinstance(sample, bool):
            return 'boolean', 'INTEGER'
        elif isinstance(sample, (dict, list)):
            return 'json', 'TEXT'
        return 'text', 'TEXT'
    
    def read_records(self, chunk_size: int = 10000) -> Generator[List[Dict], None, None]:
        self._load_data()
        
        chunk = []
        for record in self._data:
            if isinstance(record, dict):
                # Flatten nested objects to JSON strings
                flat = {}
                for k, v in record.items():
                    if isinstance(v, (dict, list)):
                        flat[k] = json.dumps(v)
                    else:
                        flat[k] = v
                chunk.append(flat)
            
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        
        if chunk:
            yield chunk
    
    def get_row_count(self) -> int:
        self._load_data()
        return len(self._data)


def get_parser(file_path: str, **kwargs) -> BaseParser:
    """Get appropriate parser based on file extension"""
    ext = os.path.splitext(file_path)[1].lower()
    
    if ext == '.csv':
        return CSVParser(file_path, **kwargs)
    elif ext in ['.xlsx', '.xls']:
        return ExcelParser(file_path, **kwargs)
    elif ext == '.json':
        return JSONParser(file_path)
    elif ext == '.yxdb':
        # Use existing YXDB parser
        from services.yxdb_parser import YxdbParser
        return YxdbParserAdapter(file_path)
    else:
        raise ValueError(f"Unsupported file format: {ext}")


class YxdbParserAdapter(BaseParser):
    """Adapter to make YxdbParser compatible with BaseParser interface"""
    
    def __init__(self, file_path: str):
        from services.yxdb_parser import YxdbParser
        self.parser = YxdbParser(file_path)
        self._schema = None
    
    def get_schema(self) -> List[ColumnInfo]:
        if self._schema:
            return self._schema
        
        raw_schema = self.parser.get_schema()
        self._schema = [
            ColumnInfo(
                name=col['name'],
                data_type=col['type'],
                sql_type=col['sql_type']
            )
            for col in raw_schema
        ]
        return self._schema
    
    def read_records(self, chunk_size: int = 10000) -> Generator[List[Dict], None, None]:
        return self.parser.read_records(chunk_size)
    
    def get_row_count(self) -> int:
        return self.parser.get_row_count()


# Supported file extensions
SUPPORTED_EXTENSIONS = {
    '.yxdb': 'Alteryx Database',
    '.csv': 'Comma-Separated Values',
    '.xlsx': 'Excel Workbook',
    '.xls': 'Excel 97-2003',
    '.json': 'JSON'
}

# Maximum file sizes (in bytes)
MAX_FILE_SIZES = {
    'default': 20 * 1024 * 1024 * 1024,  # 20 GB
    '.csv': 50 * 1024 * 1024 * 1024,      # 50 GB for CSV
    '.yxdb': 50 * 1024 * 1024 * 1024,     # 50 GB for YXDB
}
