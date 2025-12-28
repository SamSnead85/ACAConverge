"""
Database models for ACA DataHub
SQLAlchemy ORM models for users, organizations, sessions, and audit logging
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import uuid

Base = declarative_base()


def generate_uuid():
    return str(uuid.uuid4())


class Organization(Base):
    """Multi-tenant organization support"""
    __tablename__ = 'organizations'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    name = Column(String(255), nullable=False)
    slug = Column(String(100), unique=True, nullable=False)
    settings = Column(JSON, default=dict)
    branding = Column(JSON, default=dict)  # Logo, colors, etc.
    plan = Column(String(50), default='free')  # free, pro, enterprise
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    users = relationship("User", back_populates="organization")
    api_keys = relationship("APIKey", back_populates="organization")


class User(Base):
    """User accounts with role-based access"""
    __tablename__ = 'users'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    first_name = Column(String(100))
    last_name = Column(String(100))
    
    # Role: admin, analyst, viewer
    role = Column(String(50), default='viewer')
    
    # Organization membership
    organization_id = Column(String(36), ForeignKey('organizations.id'), nullable=True)
    organization = relationship("Organization", back_populates="users")
    
    # Account status
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)
    email_verified_at = Column(DateTime, nullable=True)
    
    # 2FA
    two_factor_enabled = Column(Boolean, default=False)
    two_factor_secret = Column(String(255), nullable=True)
    
    # Metadata
    avatar_url = Column(String(500), nullable=True)
    preferences = Column(JSON, default=dict)
    last_login_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    sessions = relationship("Session", back_populates="user", cascade="all, delete-orphan")
    audit_logs = relationship("AuditLog", back_populates="user")
    
    @property
    def full_name(self):
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.email.split('@')[0]


class Session(Base):
    """Active user sessions for token management"""
    __tablename__ = 'sessions'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    user_id = Column(String(36), ForeignKey('users.id'), nullable=False)
    user = relationship("User", back_populates="sessions")
    
    # Token info
    token_hash = Column(String(255), nullable=False, index=True)
    refresh_token_hash = Column(String(255), nullable=True)
    
    # Session metadata
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(String(500), nullable=True)
    device_type = Column(String(50), nullable=True)  # web, mobile, api
    
    # Expiration
    expires_at = Column(DateTime, nullable=False)
    refresh_expires_at = Column(DateTime, nullable=True)
    
    # Status
    is_active = Column(Boolean, default=True)
    revoked_at = Column(DateTime, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)


class APIKey(Base):
    """API keys for external integrations"""
    __tablename__ = 'api_keys'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    organization_id = Column(String(36), ForeignKey('organizations.id'), nullable=False)
    organization = relationship("Organization", back_populates="api_keys")
    
    name = Column(String(255), nullable=False)
    key_prefix = Column(String(10), nullable=False)  # First 8 chars for display
    key_hash = Column(String(255), nullable=False, unique=True)
    
    # Permissions
    scopes = Column(JSON, default=list)  # ['read', 'write', 'admin']
    
    # Usage tracking
    last_used_at = Column(DateTime, nullable=True)
    usage_count = Column(Integer, default=0)
    
    # Expiration
    expires_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String(36), ForeignKey('users.id'), nullable=True)


class AuditLog(Base):
    """Comprehensive audit logging for compliance"""
    __tablename__ = 'audit_logs'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    user_id = Column(String(36), ForeignKey('users.id'), nullable=True)
    user = relationship("User", back_populates="audit_logs")
    
    # Action details
    action = Column(String(100), nullable=False, index=True)  # login, query, export, etc.
    resource_type = Column(String(100), nullable=True)  # population, report, etc.
    resource_id = Column(String(36), nullable=True)
    
    # Request info
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(String(500), nullable=True)
    
    # Outcome
    status = Column(String(50), default='success')  # success, failure, error
    details = Column(JSON, default=dict)
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class EmailVerificationToken(Base):
    """Email verification and password reset tokens"""
    __tablename__ = 'email_tokens'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    user_id = Column(String(36), ForeignKey('users.id'), nullable=False)
    
    token_hash = Column(String(255), nullable=False, unique=True)
    token_type = Column(String(50), nullable=False)  # verify_email, reset_password
    
    expires_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
