"""
Security utilities for input validation and sanitization
"""

import re
import html
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Result of validation check"""
    valid: bool
    error: Optional[str] = None
    sanitized_value: Any = None


class InputValidator:
    """Validate and sanitize user inputs"""
    
    # Maximum lengths
    MAX_QUERY_LENGTH = 5000
    MAX_FILENAME_LENGTH = 255
    MAX_TABLE_NAME_LENGTH = 64
    
    # Patterns
    SAFE_FILENAME_PATTERN = re.compile(r'^[\w\-. ]+$')
    SAFE_TABLE_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    
    @classmethod
    def validate_filename(cls, filename: str) -> ValidationResult:
        """Validate uploaded filename"""
        if not filename:
            return ValidationResult(False, "Filename is required")
        
        if len(filename) > cls.MAX_FILENAME_LENGTH:
            return ValidationResult(False, f"Filename too long (max {cls.MAX_FILENAME_LENGTH})")
        
        # Check for path traversal
        if '..' in filename or '/' in filename or '\\' in filename:
            return ValidationResult(False, "Invalid characters in filename")
        
        # Sanitize filename
        safe_name = re.sub(r'[^\w\-. ]', '_', filename)
        
        return ValidationResult(True, sanitized_value=safe_name)
    
    @classmethod
    def validate_table_name(cls, name: str) -> ValidationResult:
        """Validate table name for SQL safety"""
        if not name:
            return ValidationResult(False, "Table name is required")
        
        if len(name) > cls.MAX_TABLE_NAME_LENGTH:
            return ValidationResult(False, f"Table name too long (max {cls.MAX_TABLE_NAME_LENGTH})")
        
        # Sanitize
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if safe_name[0].isdigit():
            safe_name = '_' + safe_name
        
        return ValidationResult(True, sanitized_value=safe_name)
    
    @classmethod
    def validate_natural_language_query(cls, query: str) -> ValidationResult:
        """Validate natural language query input"""
        if not query or not query.strip():
            return ValidationResult(False, "Query is required")
        
        if len(query) > cls.MAX_QUERY_LENGTH:
            return ValidationResult(False, f"Query too long (max {cls.MAX_QUERY_LENGTH} characters)")
        
        # Sanitize - remove control characters but keep normal punctuation
        sanitized = ''.join(c for c in query if c.isprintable() or c in '\n\t')
        sanitized = sanitized.strip()
        
        return ValidationResult(True, sanitized_value=sanitized)
    
    @classmethod
    def validate_job_id(cls, job_id: str) -> ValidationResult:
        """Validate job ID format"""
        if not job_id:
            return ValidationResult(False, "Job ID is required")
        
        # Job IDs should be alphanumeric with dashes
        if not re.match(r'^[a-zA-Z0-9\-]+$', job_id):
            return ValidationResult(False, "Invalid job ID format")
        
        if len(job_id) > 50:
            return ValidationResult(False, "Job ID too long")
        
        return ValidationResult(True, sanitized_value=job_id)
    
    @classmethod
    def validate_pagination(cls, page: int, limit: int, max_limit: int = 1000) -> ValidationResult:
        """Validate pagination parameters"""
        if page < 1:
            return ValidationResult(False, "Page must be at least 1")
        
        if limit < 1:
            return ValidationResult(False, "Limit must be at least 1")
        
        if limit > max_limit:
            return ValidationResult(False, f"Limit cannot exceed {max_limit}")
        
        return ValidationResult(True, sanitized_value={"page": page, "limit": limit})


class SQLInjectionPrevention:
    """SQL injection prevention utilities"""
    
    # SQL keywords that could indicate injection attempts
    DANGEROUS_PATTERNS = [
        r';\s*(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE)',
        r'--',
        r'/\*',
        r'\bUNION\s+SELECT\b',
        r'\bEXEC(UTE)?\s*\(',
        r"'\s*OR\s+'1'\s*=\s*'1",
        r'"\s*OR\s+"1"\s*=\s*"1',
        r'\bxp_\w+',
        r'\bsp_\w+',
    ]
    
    @classmethod
    def check_sql_injection(cls, query: str) -> Tuple[bool, Optional[str]]:
        """
        Check if query contains potential SQL injection patterns
        Returns (is_safe, warning_message)
        """
        if not query:
            return True, None
        
        query_upper = query.upper()
        
        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, query_upper, re.IGNORECASE):
                return False, f"Potentially dangerous SQL pattern detected"
        
        return True, None
    
    @classmethod
    def sanitize_identifier(cls, identifier: str) -> str:
        """Sanitize SQL identifier (table/column name)"""
        # Remove all non-alphanumeric except underscore
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '', identifier)
        
        # Ensure doesn't start with number
        if sanitized and sanitized[0].isdigit():
            sanitized = '_' + sanitized
        
        return sanitized or 'unnamed'
    
    @classmethod
    def quote_identifier(cls, identifier: str) -> str:
        """Safely quote an identifier for SQLite"""
        sanitized = cls.sanitize_identifier(identifier)
        return f'"{sanitized}"'


class ResponseSanitizer:
    """Sanitize response data before sending to client"""
    
    @classmethod
    def sanitize_error_message(cls, error: str) -> str:
        """Remove sensitive information from error messages"""
        # Remove file paths
        sanitized = re.sub(r'(/[\w/.-]+)+', '[path]', error)
        
        # Remove database specifics
        sanitized = re.sub(r'sqlite3?\.\w+', '[database]', sanitized)
        
        # Remove IP addresses
        sanitized = re.sub(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', '[ip]', sanitized)
        
        return sanitized
    
    @classmethod
    def sanitize_for_html(cls, text: str) -> str:
        """Escape HTML entities"""
        return html.escape(str(text))
    
    @classmethod
    def truncate_sensitive_data(cls, data: Dict, max_length: int = 1000) -> Dict:
        """Truncate long string values that might contain sensitive data"""
        result = {}
        for key, value in data.items():
            if isinstance(value, str) and len(value) > max_length:
                result[key] = value[:max_length] + '...[truncated]'
            elif isinstance(value, dict):
                result[key] = cls.truncate_sensitive_data(value, max_length)
            elif isinstance(value, list):
                result[key] = [
                    cls.truncate_sensitive_data(item, max_length) if isinstance(item, dict) else item
                    for item in value[:100]  # Limit array length too
                ]
            else:
                result[key] = value
        return result


# File type validation
ALLOWED_EXTENSIONS = {'.yxdb'}
MAX_FILE_SIZE = 15 * 1024 * 1024 * 1024  # 15GB


def validate_file_upload(filename: str, file_size: int) -> ValidationResult:
    """Validate uploaded file"""
    # Validate filename
    result = InputValidator.validate_filename(filename)
    if not result.valid:
        return result
    
    # Check extension
    ext = '.' + filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    if ext not in ALLOWED_EXTENSIONS:
        return ValidationResult(False, f"File type not allowed. Allowed: {', '.join(ALLOWED_EXTENSIONS)}")
    
    # Check size
    if file_size > MAX_FILE_SIZE:
        max_gb = MAX_FILE_SIZE / (1024 * 1024 * 1024)
        return ValidationResult(False, f"File too large. Maximum size: {max_gb:.0f}GB")
    
    return ValidationResult(True, sanitized_value=result.sanitized_value)
