"""
Role-Based Access Control (RBAC) Service for ACA DataHub
Handles permissions, decorators, and organization-scoped data isolation
"""

from functools import wraps
from typing import List, Optional, Set, Dict, Any, Callable
from enum import Enum


class Role(str, Enum):
    """User roles with hierarchical permissions"""
    ADMIN = "admin"
    ANALYST = "analyst"
    VIEWER = "viewer"


class Permission(str, Enum):
    """Granular permissions for RBAC"""
    # Data operations
    DATA_READ = "data:read"
    DATA_WRITE = "data:write"
    DATA_DELETE = "data:delete"
    DATA_EXPORT = "data:export"
    
    # Query operations
    QUERY_EXECUTE = "query:execute"
    QUERY_SAVE = "query:save"
    
    # Population management
    POPULATION_READ = "population:read"
    POPULATION_WRITE = "population:write"
    POPULATION_DELETE = "population:delete"
    
    # Messaging
    MESSAGE_SEND = "message:send"
    MESSAGE_TEMPLATE = "message:template"
    
    # Campaigns
    CAMPAIGN_READ = "campaign:read"
    CAMPAIGN_WRITE = "campaign:write"
    CAMPAIGN_SEND = "campaign:send"
    
    # Reports
    REPORT_READ = "report:read"
    REPORT_WRITE = "report:write"
    REPORT_EXPORT = "report:export"
    
    # Integrations
    INTEGRATION_READ = "integration:read"
    INTEGRATION_WRITE = "integration:write"
    
    # Admin operations
    USER_READ = "user:read"
    USER_WRITE = "user:write"
    USER_DELETE = "user:delete"
    ORG_SETTINGS = "org:settings"
    AUDIT_READ = "audit:read"
    API_KEYS = "api_keys:manage"


# Role-to-Permission mapping
ROLE_PERMISSIONS: Dict[Role, Set[Permission]] = {
    Role.ADMIN: set(Permission),  # Admin has all permissions
    
    Role.ANALYST: {
        Permission.DATA_READ,
        Permission.DATA_WRITE,
        Permission.DATA_EXPORT,
        Permission.QUERY_EXECUTE,
        Permission.QUERY_SAVE,
        Permission.POPULATION_READ,
        Permission.POPULATION_WRITE,
        Permission.POPULATION_DELETE,
        Permission.MESSAGE_SEND,
        Permission.MESSAGE_TEMPLATE,
        Permission.CAMPAIGN_READ,
        Permission.CAMPAIGN_WRITE,
        Permission.CAMPAIGN_SEND,
        Permission.REPORT_READ,
        Permission.REPORT_WRITE,
        Permission.REPORT_EXPORT,
        Permission.INTEGRATION_READ,
    },
    
    Role.VIEWER: {
        Permission.DATA_READ,
        Permission.QUERY_EXECUTE,
        Permission.POPULATION_READ,
        Permission.CAMPAIGN_READ,
        Permission.REPORT_READ,
        Permission.INTEGRATION_READ,
    }
}


class RBACService:
    """RBAC enforcement service"""
    
    def __init__(self):
        self._custom_permissions: Dict[str, Set[Permission]] = {}
    
    def get_role_permissions(self, role: str) -> Set[Permission]:
        """Get all permissions for a role"""
        try:
            role_enum = Role(role)
            return ROLE_PERMISSIONS.get(role_enum, set())
        except ValueError:
            return set()
    
    def has_permission(self, user_role: str, permission: Permission) -> bool:
        """Check if a role has a specific permission"""
        permissions = self.get_role_permissions(user_role)
        return permission in permissions
    
    def has_any_permission(self, user_role: str, permissions: List[Permission]) -> bool:
        """Check if a role has any of the specified permissions"""
        role_permissions = self.get_role_permissions(user_role)
        return any(p in role_permissions for p in permissions)
    
    def has_all_permissions(self, user_role: str, permissions: List[Permission]) -> bool:
        """Check if a role has all of the specified permissions"""
        role_permissions = self.get_role_permissions(user_role)
        return all(p in role_permissions for p in permissions)
    
    def get_user_permissions(self, user_id: str, user_role: str) -> Set[Permission]:
        """Get combined role + custom permissions for a user"""
        permissions = self.get_role_permissions(user_role).copy()
        
        # Add any custom user-specific permissions
        if user_id in self._custom_permissions:
            permissions.update(self._custom_permissions[user_id])
        
        return permissions
    
    def grant_permission(self, user_id: str, permission: Permission) -> None:
        """Grant a custom permission to a user"""
        if user_id not in self._custom_permissions:
            self._custom_permissions[user_id] = set()
        self._custom_permissions[user_id].add(permission)
    
    def revoke_permission(self, user_id: str, permission: Permission) -> None:
        """Revoke a custom permission from a user"""
        if user_id in self._custom_permissions:
            self._custom_permissions[user_id].discard(permission)


# Singleton instance
rbac_service = RBACService()


# =========================================================================
# FastAPI Dependency Injection Helpers
# =========================================================================

def require_permission(permission: Permission):
    """
    FastAPI dependency to require a specific permission.
    
    Usage:
        @router.get("/protected")
        async def protected_route(user = Depends(require_permission(Permission.DATA_READ))):
            return {"message": "Access granted"}
    """
    async def permission_checker(request):
        from fastapi import HTTPException, Request
        
        # Get user from request state (set by auth middleware)
        user = getattr(request.state, 'user', None)
        
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        
        user_role = user.get('role', 'viewer')
        
        if not rbac_service.has_permission(user_role, permission):
            raise HTTPException(
                status_code=403, 
                detail=f"Permission denied: {permission.value} required"
            )
        
        return user
    
    return permission_checker


def require_any_permission(permissions: List[Permission]):
    """Require any one of the specified permissions"""
    async def permission_checker(request):
        from fastapi import HTTPException
        
        user = getattr(request.state, 'user', None)
        
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        
        user_role = user.get('role', 'viewer')
        
        if not rbac_service.has_any_permission(user_role, permissions):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        return user
    
    return permission_checker


def require_role(allowed_roles: List[str]):
    """
    FastAPI dependency to require specific role(s).
    
    Usage:
        @router.get("/admin-only")
        async def admin_route(user = Depends(require_role(["admin"]))):
            return {"message": "Admin access granted"}
    """
    async def role_checker(request):
        from fastapi import HTTPException
        
        user = getattr(request.state, 'user', None)
        
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        
        user_role = user.get('role', 'viewer')
        
        if user_role not in allowed_roles:
            raise HTTPException(
                status_code=403,
                detail=f"Role not authorized. Required: {', '.join(allowed_roles)}"
            )
        
        return user
    
    return role_checker


# =========================================================================
# Organization Scoping
# =========================================================================

class OrganizationScope:
    """Helper for organization-scoped data isolation"""
    
    @staticmethod
    def filter_by_org(query, org_id: Optional[str], model_class):
        """
        Add organization filter to a SQLAlchemy query.
        
        Usage:
            query = session.query(Population)
            query = OrganizationScope.filter_by_org(query, user.org_id, Population)
        """
        if org_id and hasattr(model_class, 'organization_id'):
            return query.filter(model_class.organization_id == org_id)
        return query
    
    @staticmethod
    def validate_resource_access(
        resource_org_id: Optional[str],
        user_org_id: Optional[str],
        user_role: str
    ) -> bool:
        """
        Check if a user can access a resource based on organization.
        Admins can access all resources within their org.
        """
        # Super admin (no org restriction)
        if user_org_id is None and user_role == Role.ADMIN.value:
            return True
        
        # Same organization
        if resource_org_id == user_org_id:
            return True
        
        # Public resource (no org)
        if resource_org_id is None:
            return True
        
        return False


# =========================================================================
# Audit Logging Decorator
# =========================================================================

def audit_log(action: str, resource_type: str = None):
    """
    Decorator to automatically log actions for compliance.
    
    Usage:
        @audit_log("population.create", "population")
        async def create_population(request, data):
            ...
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract request from args
            request = kwargs.get('request') or (args[0] if args else None)
            user = getattr(request.state, 'user', None) if request else None
            
            result = None
            error = None
            
            try:
                result = await func(*args, **kwargs)
                status = "success"
            except Exception as e:
                error = str(e)
                status = "error"
                raise
            finally:
                # Log the action
                log_entry = {
                    "user_id": user.get("sub") if user else None,
                    "action": action,
                    "resource_type": resource_type,
                    "status": status,
                    "error": error
                }
                # In production, save to AuditLog table
                print(f"[AUDIT] {log_entry}")
            
            return result
        return wrapper
    return decorator
