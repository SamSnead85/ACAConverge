"""
Synthetic Data Routes for ACA DataHub
Data generation, privacy-preserving synthesis, masking, and anonymization
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import string
import hashlib

router = APIRouter(prefix="/synthetic", tags=["Synthetic Data"])


# =========================================================================
# Models
# =========================================================================

class GenerationType(str, Enum):
    RANDOM = "random"
    PATTERN = "pattern"
    DISTRIBUTION = "distribution"
    REFERENCE = "reference"


class MaskingType(str, Enum):
    REDACT = "redact"
    HASH = "hash"
    PARTIAL = "partial"
    SUBSTITUTE = "substitute"
    SHUFFLE = "shuffle"


# =========================================================================
# Data Generator
# =========================================================================

class SyntheticDataGenerator:
    """Generate synthetic test data"""
    
    def __init__(self):
        self.jobs: Dict[str, dict] = {}
        self.schemas: Dict[str, dict] = {}
        self._counter = 0
        
        # Sample data pools
        self.first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"]
        self.last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
        self.states = ["GA", "FL", "TX", "CA", "NY", "IL", "PA", "OH", "MI", "NC"]
        self.cities = ["Atlanta", "Miami", "Houston", "Los Angeles", "New York", "Chicago", "Philadelphia", "Columbus", "Detroit", "Charlotte"]
        self.domains = ["gmail.com", "yahoo.com", "outlook.com", "email.com", "mail.com"]
    
    def generate_value(self, field_type: str, config: dict = None) -> Any:
        """Generate a synthetic value based on field type"""
        config = config or {}
        
        if field_type == "first_name":
            return random.choice(self.first_names)
        elif field_type == "last_name":
            return random.choice(self.last_names)
        elif field_type == "email":
            first = random.choice(self.first_names).lower()
            last = random.choice(self.last_names).lower()
            domain = random.choice(self.domains)
            return f"{first}.{last}{random.randint(1, 999)}@{domain}"
        elif field_type == "phone":
            return f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"
        elif field_type == "ssn":
            return f"XXX-XX-{random.randint(1000, 9999)}"
        elif field_type == "address":
            return f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Park', 'Cedar'])} {random.choice(['St', 'Ave', 'Blvd', 'Dr'])}"
        elif field_type == "city":
            return random.choice(self.cities)
        elif field_type == "state":
            return random.choice(self.states)
        elif field_type == "zip":
            return f"{random.randint(10000, 99999)}"
        elif field_type == "date":
            days_ago = random.randint(0, config.get("max_days", 365))
            return (datetime.utcnow() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        elif field_type == "integer":
            return random.randint(config.get("min", 0), config.get("max", 100))
        elif field_type == "float":
            return round(random.uniform(config.get("min", 0), config.get("max", 100)), 2)
        elif field_type == "boolean":
            return random.choice([True, False])
        elif field_type == "income":
            return random.randint(20000, 200000)
        elif field_type == "score":
            return random.randint(0, 100)
        elif field_type == "uuid":
            return f"{random.randint(10000000, 99999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
        else:
            return ''.join(random.choices(string.ascii_letters, k=10))
    
    def generate_record(self, schema: dict) -> dict:
        """Generate a single synthetic record"""
        record = {}
        for field_name, field_config in schema.get("fields", {}).items():
            field_type = field_config.get("type", "string")
            record[field_name] = self.generate_value(field_type, field_config)
        return record
    
    def generate_dataset(
        self,
        schema: dict,
        count: int,
        preserve_distribution: bool = False
    ) -> dict:
        """Generate a synthetic dataset"""
        self._counter += 1
        job_id = f"gen_{self._counter}"
        
        records = [self.generate_record(schema) for _ in range(count)]
        
        job = {
            "id": job_id,
            "status": "completed",
            "schema": schema,
            "record_count": count,
            "preserve_distribution": preserve_distribution,
            "created_at": datetime.utcnow().isoformat(),
            "records": records
        }
        
        self.jobs[job_id] = job
        return job
    
    def generate_with_referential_integrity(
        self,
        schemas: Dict[str, dict],
        counts: Dict[str, int],
        relationships: List[dict]
    ) -> dict:
        """Generate datasets with referential integrity"""
        self._counter += 1
        job_id = f"gen_ri_{self._counter}"
        
        datasets = {}
        
        # Generate primary tables first
        for table_name, schema in schemas.items():
            count = counts.get(table_name, 100)
            records = [self.generate_record(schema) for _ in range(count)]
            
            # Add IDs
            for i, record in enumerate(records):
                record["id"] = f"{table_name}_{i+1}"
            
            datasets[table_name] = records
        
        # Apply relationships
        for rel in relationships:
            parent = rel.get("parent")
            child = rel.get("child")
            fk_field = rel.get("foreign_key", f"{parent}_id")
            
            if parent in datasets and child in datasets:
                parent_ids = [r["id"] for r in datasets[parent]]
                for record in datasets[child]:
                    record[fk_field] = random.choice(parent_ids)
        
        return {
            "id": job_id,
            "datasets": datasets,
            "relationships": relationships,
            "created_at": datetime.utcnow().isoformat()
        }


generator = SyntheticDataGenerator()


# =========================================================================
# Data Masker
# =========================================================================

class DataMasker:
    """Mask and anonymize sensitive data"""
    
    def __init__(self):
        self.rules: Dict[str, dict] = {}
        self._counter = 0
    
    def create_rule(
        self,
        name: str,
        field_pattern: str,
        masking_type: str,
        config: dict = None
    ) -> dict:
        self._counter += 1
        rule_id = f"rule_{self._counter}"
        
        rule = {
            "id": rule_id,
            "name": name,
            "field_pattern": field_pattern,
            "masking_type": masking_type,
            "config": config or {},
            "created_at": datetime.utcnow().isoformat()
        }
        
        self.rules[rule_id] = rule
        return rule
    
    def mask_value(self, value: Any, masking_type: str, config: dict = None) -> str:
        """Mask a single value"""
        config = config or {}
        value_str = str(value)
        
        if masking_type == "redact":
            return "***REDACTED***"
        
        elif masking_type == "hash":
            return hashlib.sha256(value_str.encode()).hexdigest()[:16]
        
        elif masking_type == "partial":
            visible_chars = config.get("visible_chars", 4)
            position = config.get("position", "end")  # start, end, middle
            
            if len(value_str) <= visible_chars:
                return "*" * len(value_str)
            
            if position == "start":
                return value_str[:visible_chars] + "*" * (len(value_str) - visible_chars)
            elif position == "end":
                return "*" * (len(value_str) - visible_chars) + value_str[-visible_chars:]
            else:
                mid = len(value_str) // 2
                half = visible_chars // 2
                return "*" * (mid - half) + value_str[mid-half:mid+half] + "*" * (len(value_str) - mid - half)
        
        elif masking_type == "substitute":
            # Replace with fake but consistent value
            seed = hash(value_str)
            random.seed(seed)
            
            if "@" in value_str:  # Email
                return f"user{random.randint(1000, 9999)}@masked.com"
            elif value_str.replace("-", "").isdigit() and len(value_str) in [10, 12]:  # Phone
                return f"(555) 555-{random.randint(1000, 9999)}"
            else:
                return f"MASKED_{random.randint(10000, 99999)}"
        
        elif masking_type == "shuffle":
            chars = list(value_str)
            random.shuffle(chars)
            return ''.join(chars)
        
        return value_str
    
    def mask_record(self, record: dict, rules: List[dict]) -> dict:
        """Mask a record according to rules"""
        masked = record.copy()
        
        for field, value in record.items():
            for rule in rules:
                # Simple pattern matching
                if rule["field_pattern"] in field.lower():
                    masked[field] = self.mask_value(value, rule["masking_type"], rule.get("config"))
                    break
        
        return masked
    
    def mask_dataset(self, records: List[dict], rule_ids: List[str] = None) -> dict:
        """Mask a dataset"""
        if rule_ids:
            rules = [self.rules[rid] for rid in rule_ids if rid in self.rules]
        else:
            rules = list(self.rules.values())
        
        masked_records = [self.mask_record(r, rules) for r in records]
        
        return {
            "original_count": len(records),
            "masked_count": len(masked_records),
            "rules_applied": len(rules),
            "records": masked_records
        }


masker = DataMasker()


# =========================================================================
# Data Quality Validator
# =========================================================================

class SyntheticValidator:
    """Validate synthetic data quality"""
    
    def validate_statistical_preservation(
        self,
        original: List[dict],
        synthetic: List[dict],
        numeric_fields: List[str]
    ) -> dict:
        """Check if synthetic data preserves statistical properties"""
        checks = []
        
        for field in numeric_fields:
            orig_values = [r.get(field, 0) for r in original if isinstance(r.get(field), (int, float))]
            synth_values = [r.get(field, 0) for r in synthetic if isinstance(r.get(field), (int, float))]
            
            if orig_values and synth_values:
                orig_mean = sum(orig_values) / len(orig_values)
                synth_mean = sum(synth_values) / len(synth_values)
                
                orig_std = (sum((x - orig_mean) ** 2 for x in orig_values) / len(orig_values)) ** 0.5
                synth_std = (sum((x - synth_mean) ** 2 for x in synth_values) / len(synth_values)) ** 0.5
                
                mean_diff = abs(orig_mean - synth_mean) / (orig_mean if orig_mean else 1) * 100
                std_diff = abs(orig_std - synth_std) / (orig_std if orig_std else 1) * 100
                
                checks.append({
                    "field": field,
                    "original_mean": round(orig_mean, 2),
                    "synthetic_mean": round(synth_mean, 2),
                    "mean_difference_percent": round(mean_diff, 2),
                    "original_std": round(orig_std, 2),
                    "synthetic_std": round(synth_std, 2),
                    "std_difference_percent": round(std_diff, 2),
                    "passed": mean_diff < 10 and std_diff < 20
                })
        
        return {
            "checks": checks,
            "overall_passed": all(c["passed"] for c in checks)
        }
    
    def validate_uniqueness(self, records: List[dict], unique_fields: List[str]) -> dict:
        """Check uniqueness constraints"""
        checks = []
        
        for field in unique_fields:
            values = [r.get(field) for r in records if r.get(field)]
            unique_values = set(values)
            
            uniqueness_rate = len(unique_values) / len(values) if values else 0
            
            checks.append({
                "field": field,
                "total_values": len(values),
                "unique_values": len(unique_values),
                "uniqueness_rate": round(uniqueness_rate, 4),
                "passed": uniqueness_rate >= 0.99
            })
        
        return {"checks": checks}


validator = SyntheticValidator()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/generate")
async def generate_dataset(
    schema: dict,
    count: int = Query(default=100, le=10000),
    preserve_distribution: bool = Query(default=False)
):
    """Generate synthetic dataset"""
    return generator.generate_dataset(schema, count, preserve_distribution)


@router.post("/generate/value")
async def generate_value(
    field_type: str = Query(...),
    count: int = Query(default=10, le=100)
):
    """Generate synthetic values"""
    values = [generator.generate_value(field_type) for _ in range(count)]
    return {"field_type": field_type, "values": values}


@router.post("/generate/referential")
async def generate_with_relationships(
    schemas: Dict[str, dict],
    counts: Dict[str, int],
    relationships: List[dict] = []
):
    """Generate datasets with referential integrity"""
    return generator.generate_with_referential_integrity(schemas, counts, relationships)


@router.get("/jobs")
async def list_generation_jobs():
    """List generation jobs"""
    # Remove records from response for listing
    jobs = []
    for job in generator.jobs.values():
        j = {k: v for k, v in job.items() if k != "records"}
        jobs.append(j)
    return {"jobs": jobs}


@router.get("/jobs/{job_id}")
async def get_generation_job(job_id: str, include_records: bool = Query(default=False)):
    """Get generation job details"""
    job = generator.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if not include_records:
        return {k: v for k, v in job.items() if k != "records"}
    return job


@router.post("/mask/rules")
async def create_masking_rule(
    name: str = Query(...),
    field_pattern: str = Query(...),
    masking_type: MaskingType = Query(...),
    config: dict = None
):
    """Create masking rule"""
    rule = masker.create_rule(name, field_pattern, masking_type.value, config)
    return {"success": True, "rule": rule}


@router.get("/mask/rules")
async def list_masking_rules():
    """List masking rules"""
    return {"rules": list(masker.rules.values())}


@router.post("/mask")
async def mask_dataset(
    records: List[dict],
    rule_ids: List[str] = None
):
    """Mask a dataset"""
    return masker.mask_dataset(records, rule_ids)


@router.post("/mask/value")
async def mask_value(
    value: str = Query(...),
    masking_type: MaskingType = Query(...)
):
    """Mask a single value"""
    masked = masker.mask_value(value, masking_type.value)
    return {"original": value, "masked": masked, "type": masking_type.value}


@router.post("/validate/statistics")
async def validate_statistics(
    original: List[dict],
    synthetic: List[dict],
    numeric_fields: List[str] = Query(...)
):
    """Validate statistical preservation"""
    return validator.validate_statistical_preservation(original, synthetic, numeric_fields)


@router.post("/validate/uniqueness")
async def validate_uniqueness(
    records: List[dict],
    unique_fields: List[str] = Query(...)
):
    """Validate uniqueness constraints"""
    return validator.validate_uniqueness(records, unique_fields)
