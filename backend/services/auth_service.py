"""
Authentication Service for ACA DataHub
Handles user registration, login, JWT tokens, 2FA, and session management
"""

import os
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any

import bcrypt
import pyotp
from jose import jwt, JWTError

# Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", secrets.token_urlsafe(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7


class AuthService:
    """Authentication and authorization service"""
    
    def __init__(self, db_session=None):
        self.db = db_session
        self._users_store = {}  # In-memory store for demo (replace with DB)
        self._sessions_store = {}
        self._tokens_store = {}
    
    # =========================================================================
    # Password Hashing
    # =========================================================================
    
    def hash_password(self, password: str) -> str:
        """Hash a password using bcrypt"""
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    
    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify a password against its hash"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
        except Exception:
            return False
    
    # =========================================================================
    # JWT Token Management
    # =========================================================================
    
    def create_access_token(
        self, 
        user_id: str, 
        email: str, 
        role: str,
        org_id: Optional[str] = None,
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """Create a JWT access token"""
        expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
        
        payload = {
            "sub": user_id,
            "email": email,
            "role": role,
            "org_id": org_id,
            "type": "access",
            "exp": expire,
            "iat": datetime.utcnow()
        }
        
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    
    def create_refresh_token(self, user_id: str) -> str:
        """Create a refresh token"""
        expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        
        payload = {
            "sub": user_id,
            "type": "refresh",
            "exp": expire,
            "iat": datetime.utcnow(),
            "jti": secrets.token_urlsafe(16)  # Unique token ID
        }
        
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    
    def decode_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Decode and validate a JWT token"""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            return payload
        except JWTError:
            return None
    
    def create_token_pair(
        self, 
        user_id: str, 
        email: str, 
        role: str,
        org_id: Optional[str] = None
    ) -> Tuple[str, str]:
        """Create both access and refresh tokens"""
        access_token = self.create_access_token(user_id, email, role, org_id)
        refresh_token = self.create_refresh_token(user_id)
        return access_token, refresh_token
    
    # =========================================================================
    # User Registration & Login
    # =========================================================================
    
    def register_user(
        self,
        email: str,
        password: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        role: str = "viewer"
    ) -> Dict[str, Any]:
        """Register a new user"""
        # Check if user exists
        if email.lower() in self._users_store:
            raise ValueError("User with this email already exists")
        
        # Validate password strength
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters")
        
        # Create user
        user_id = secrets.token_urlsafe(16)
        user = {
            "id": user_id,
            "email": email.lower(),
            "password_hash": self.hash_password(password),
            "first_name": first_name,
            "last_name": last_name,
            "role": role,
            "organization_id": None,
            "is_active": True,
            "is_verified": False,
            "two_factor_enabled": False,
            "two_factor_secret": None,
            "created_at": datetime.utcnow().isoformat()
        }
        
        self._users_store[email.lower()] = user
        
        # Generate verification token
        verification_token = self.create_verification_token(user_id, "verify_email")
        
        return {
            "user": {k: v for k, v in user.items() if k != "password_hash"},
            "verification_token": verification_token
        }
    
    def login(
        self,
        email: str,
        password: str,
        totp_code: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> Dict[str, Any]:
        """Authenticate a user and create session"""
        email = email.lower()
        
        # Find user
        user = self._users_store.get(email)
        if not user:
            raise ValueError("Invalid email or password")
        
        # Verify password
        if not self.verify_password(password, user["password_hash"]):
            raise ValueError("Invalid email or password")
        
        # Check if account is active
        if not user.get("is_active", True):
            raise ValueError("Account is disabled")
        
        # Check 2FA if enabled
        if user.get("two_factor_enabled"):
            if not totp_code:
                return {"requires_2fa": True, "user_id": user["id"]}
            
            if not self.verify_totp(user["two_factor_secret"], totp_code):
                raise ValueError("Invalid 2FA code")
        
        # Create tokens
        access_token, refresh_token = self.create_token_pair(
            user["id"],
            user["email"],
            user["role"],
            user.get("organization_id")
        )
        
        # Create session
        session_id = secrets.token_urlsafe(16)
        self._sessions_store[session_id] = {
            "user_id": user["id"],
            "token_hash": hashlib.sha256(access_token.encode()).hexdigest(),
            "ip_address": ip_address,
            "user_agent": user_agent,
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)).isoformat()
        }
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "user": {k: v for k, v in user.items() if k not in ["password_hash", "two_factor_secret"]}
        }
    
    def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """Generate new access token using refresh token"""
        payload = self.decode_token(refresh_token)
        
        if not payload or payload.get("type") != "refresh":
            raise ValueError("Invalid refresh token")
        
        user_id = payload.get("sub")
        
        # Find user
        user = None
        for u in self._users_store.values():
            if u["id"] == user_id:
                user = u
                break
        
        if not user:
            raise ValueError("User not found")
        
        # Create new access token
        access_token = self.create_access_token(
            user["id"],
            user["email"],
            user["role"],
            user.get("organization_id")
        )
        
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
        }
    
    def logout(self, user_id: str, session_id: Optional[str] = None) -> bool:
        """Invalidate user session(s)"""
        if session_id:
            # Revoke specific session
            if session_id in self._sessions_store:
                del self._sessions_store[session_id]
        else:
            # Revoke all sessions for user
            to_delete = [
                sid for sid, session in self._sessions_store.items()
                if session["user_id"] == user_id
            ]
            for sid in to_delete:
                del self._sessions_store[sid]
        
        return True
    
    # =========================================================================
    # Two-Factor Authentication
    # =========================================================================
    
    def setup_2fa(self, user_id: str) -> Dict[str, str]:
        """Generate 2FA secret and provisioning URI"""
        secret = pyotp.random_base32()
        
        # Find user
        user = None
        for u in self._users_store.values():
            if u["id"] == user_id:
                user = u
                break
        
        if not user:
            raise ValueError("User not found")
        
        # Generate provisioning URI for authenticator apps
        totp = pyotp.TOTP(secret)
        provisioning_uri = totp.provisioning_uri(
            name=user["email"],
            issuer_name="ACA DataHub"
        )
        
        return {
            "secret": secret,
            "provisioning_uri": provisioning_uri
        }
    
    def enable_2fa(self, user_id: str, secret: str, code: str) -> bool:
        """Verify code and enable 2FA for user"""
        if not self.verify_totp(secret, code):
            raise ValueError("Invalid verification code")
        
        # Update user
        for email, user in self._users_store.items():
            if user["id"] == user_id:
                user["two_factor_enabled"] = True
                user["two_factor_secret"] = secret
                return True
        
        raise ValueError("User not found")
    
    def disable_2fa(self, user_id: str, password: str) -> bool:
        """Disable 2FA for user (requires password confirmation)"""
        for email, user in self._users_store.items():
            if user["id"] == user_id:
                if not self.verify_password(password, user["password_hash"]):
                    raise ValueError("Invalid password")
                
                user["two_factor_enabled"] = False
                user["two_factor_secret"] = None
                return True
        
        raise ValueError("User not found")
    
    def verify_totp(self, secret: str, code: str) -> bool:
        """Verify a TOTP code"""
        if not secret:
            return False
        totp = pyotp.TOTP(secret)
        return totp.verify(code, valid_window=1)
    
    # =========================================================================
    # Email Verification & Password Reset
    # =========================================================================
    
    def create_verification_token(self, user_id: str, token_type: str) -> str:
        """Create an email verification or password reset token"""
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        
        self._tokens_store[token_hash] = {
            "user_id": user_id,
            "type": token_type,
            "expires_at": (datetime.utcnow() + timedelta(hours=24)).isoformat(),
            "used": False
        }
        
        return token
    
    def verify_email(self, token: str) -> bool:
        """Verify email address using token"""
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        token_data = self._tokens_store.get(token_hash)
        
        if not token_data:
            raise ValueError("Invalid token")
        
        if token_data["used"]:
            raise ValueError("Token already used")
        
        if datetime.fromisoformat(token_data["expires_at"]) < datetime.utcnow():
            raise ValueError("Token expired")
        
        if token_data["type"] != "verify_email":
            raise ValueError("Invalid token type")
        
        # Update user
        for email, user in self._users_store.items():
            if user["id"] == token_data["user_id"]:
                user["is_verified"] = True
                user["email_verified_at"] = datetime.utcnow().isoformat()
                token_data["used"] = True
                return True
        
        raise ValueError("User not found")
    
    def request_password_reset(self, email: str) -> Optional[str]:
        """Generate password reset token"""
        email = email.lower()
        user = self._users_store.get(email)
        
        if not user:
            # Don't reveal if email exists
            return None
        
        return self.create_verification_token(user["id"], "reset_password")
    
    def reset_password(self, token: str, new_password: str) -> bool:
        """Reset password using token"""
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        token_data = self._tokens_store.get(token_hash)
        
        if not token_data:
            raise ValueError("Invalid token")
        
        if token_data["used"]:
            raise ValueError("Token already used")
        
        if datetime.fromisoformat(token_data["expires_at"]) < datetime.utcnow():
            raise ValueError("Token expired")
        
        if token_data["type"] != "reset_password":
            raise ValueError("Invalid token type")
        
        if len(new_password) < 8:
            raise ValueError("Password must be at least 8 characters")
        
        # Update user password
        for email, user in self._users_store.items():
            if user["id"] == token_data["user_id"]:
                user["password_hash"] = self.hash_password(new_password)
                token_data["used"] = True
                
                # Invalidate all sessions
                self.logout(user["id"])
                return True
        
        raise ValueError("User not found")
    
    # =========================================================================
    # User Management
    # =========================================================================
    
    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        for user in self._users_store.values():
            if user["id"] == user_id:
                return {k: v for k, v in user.items() if k not in ["password_hash", "two_factor_secret"]}
        return None
    
    def update_user(self, user_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update user profile"""
        allowed_fields = ["first_name", "last_name", "avatar_url", "preferences"]
        
        for email, user in self._users_store.items():
            if user["id"] == user_id:
                for key, value in updates.items():
                    if key in allowed_fields:
                        user[key] = value
                return {k: v for k, v in user.items() if k not in ["password_hash", "two_factor_secret"]}
        
        raise ValueError("User not found")
    
    def change_password(self, user_id: str, current_password: str, new_password: str) -> bool:
        """Change user password"""
        for email, user in self._users_store.items():
            if user["id"] == user_id:
                if not self.verify_password(current_password, user["password_hash"]):
                    raise ValueError("Current password is incorrect")
                
                if len(new_password) < 8:
                    raise ValueError("New password must be at least 8 characters")
                
                user["password_hash"] = self.hash_password(new_password)
                return True
        
        raise ValueError("User not found")
    
    def get_user_sessions(self, user_id: str) -> list:
        """Get all active sessions for a user"""
        sessions = []
        for session_id, session in self._sessions_store.items():
            if session["user_id"] == user_id:
                sessions.append({
                    "id": session_id,
                    "ip_address": session.get("ip_address"),
                    "user_agent": session.get("user_agent"),
                    "created_at": session.get("created_at"),
                    "expires_at": session.get("expires_at")
                })
        return sessions


# Singleton instance
auth_service = AuthService()
