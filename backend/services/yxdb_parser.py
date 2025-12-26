"""
YXDB Parser Service
Handles reading and parsing .yxdb files using the yxdb library
"""

from typing import Generator, Dict, List, Any, Optional
from dataclasses import dataclass
import os


@dataclass
class FieldInfo:
    """Information about a field in the YXDB file"""
    name: str
    type: str
    size: int = 0
    scale: int = 0


class YxdbParser:
    """
    Parser for Alteryx .yxdb files
    Provides streaming access to records for memory-efficient processing
    """
    
    # Type mapping from YXDB types to SQLite types
    TYPE_MAPPING = {
        'Bool': 'INTEGER',
        'Byte': 'INTEGER',
        'Int16': 'INTEGER',
        'Int32': 'INTEGER',
        'Int64': 'INTEGER',
        'FixedDecimal': 'REAL',
        'Float': 'REAL',
        'Double': 'REAL',
        'String': 'TEXT',
        'WString': 'TEXT',
        'V_String': 'TEXT',
        'V_WString': 'TEXT',
        'Date': 'TEXT',
        'DateTime': 'TEXT',
        'Time': 'TEXT',
        'Blob': 'BLOB',
        'SpatialObj': 'TEXT',
    }

    def __init__(self, file_path: str):
        """
        Initialize the parser with a file path
        
        Args:
            file_path: Path to the .yxdb file
        """
        self.file_path = file_path
        self.reader = None
        self.fields: List[FieldInfo] = []
        self.record_count: int = 0
        self._initialize()

    def _initialize(self):
        """Initialize the reader and extract metadata"""
        try:
            from yxdb import YxdbReader
            self.reader = YxdbReader(path=self.file_path)
            self._extract_fields()
        except ImportError:
            # Fallback for testing without yxdb installed
            raise ImportError(
                "yxdb library not installed. Install with: pip install yxdb"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to open YXDB file: {str(e)}")

    def _extract_fields(self):
        """Extract field information from the YXDB file"""
        raw_fields = self.reader.list_fields()
        self.fields = []
        for field in raw_fields:
            self.fields.append(FieldInfo(
                name=field.name,
                type=field.type,
                size=getattr(field, 'size', 0),
                scale=getattr(field, 'scale', 0)
            ))

    def get_schema(self) -> List[Dict[str, Any]]:
        """
        Get the schema of the YXDB file
        
        Returns:
            List of field definitions with name, original type, and SQL type
        """
        return [
            {
                "name": field.name,
                "original_type": field.type,
                "sql_type": self.TYPE_MAPPING.get(field.type, 'TEXT'),
                "size": field.size,
                "scale": field.scale
            }
            for field in self.fields
        ]

    def get_field_names(self) -> List[str]:
        """Get list of field names"""
        return [field.name for field in self.fields]

    def stream_records(self, batch_size: int = 10000) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Stream records from the YXDB file in batches
        
        Args:
            batch_size: Number of records per batch
            
        Yields:
            List of record dictionaries
        """
        batch = []
        count = 0
        
        while self.reader.next():
            record = {}
            for field in self.fields:
                try:
                    value = self.reader.read_name(field.name)
                    # Handle spatial objects specially
                    if field.type == 'SpatialObj' and value is not None:
                        try:
                            from yxdb.spatial import to_geojson
                            value = to_geojson(value)
                        except:
                            value = str(value)
                    record[field.name] = value
                except Exception:
                    record[field.name] = None
            
            batch.append(record)
            count += 1
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield remaining records
        if batch:
            yield batch
        
        self.record_count = count

    def get_file_size(self) -> int:
        """Get the size of the YXDB file in bytes"""
        return os.path.getsize(self.file_path)

    def close(self):
        """Close the reader"""
        self.reader = None


class MockYxdbParser:
    """
    Mock parser for testing without actual .yxdb files
    Generates sample data for development and testing
    """
    
    def __init__(self, file_path: str, num_records: int = 10000):
        self.file_path = file_path
        self.num_records = num_records
        self.fields = [
            FieldInfo(name="id", type="Int64"),
            FieldInfo(name="name", type="V_WString", size=100),
            FieldInfo(name="email", type="V_WString", size=200),
            FieldInfo(name="sales", type="Double"),
            FieldInfo(name="region", type="V_WString", size=50),
            FieldInfo(name="date", type="Date"),
            FieldInfo(name="active", type="Bool"),
        ]
        self.record_count = 0

    def get_schema(self) -> List[Dict[str, Any]]:
        """Get mock schema"""
        type_mapping = {
            'Bool': 'INTEGER',
            'Int64': 'INTEGER',
            'Double': 'REAL',
            'V_WString': 'TEXT',
            'Date': 'TEXT',
        }
        return [
            {
                "name": field.name,
                "original_type": field.type,
                "sql_type": type_mapping.get(field.type, 'TEXT'),
                "size": field.size,
                "scale": field.scale
            }
            for field in self.fields
        ]

    def get_field_names(self) -> List[str]:
        """Get list of field names"""
        return [field.name for field in self.fields]

    def stream_records(self, batch_size: int = 10000) -> Generator[List[Dict[str, Any]], None, None]:
        """Generate mock records"""
        import random
        from datetime import datetime, timedelta
        
        regions = ["North", "South", "East", "West", "Central"]
        names = ["Alice Johnson", "Bob Smith", "Carol Williams", "David Brown", "Eva Davis",
                 "Frank Miller", "Grace Wilson", "Henry Moore", "Iris Taylor", "Jack Anderson"]
        
        batch = []
        base_date = datetime(2020, 1, 1)
        
        for i in range(self.num_records):
            record = {
                "id": i + 1,
                "name": random.choice(names),
                "email": f"user{i+1}@example.com",
                "sales": round(random.uniform(100, 50000), 2),
                "region": random.choice(regions),
                "date": (base_date + timedelta(days=random.randint(0, 1000))).strftime("%Y-%m-%d"),
                "active": random.choice([0, 1])
            }
            batch.append(record)
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        if batch:
            yield batch
        
        self.record_count = self.num_records

    def get_file_size(self) -> int:
        """Return mock file size"""
        return self.num_records * 200  # Approximate bytes per record

    def close(self):
        pass
