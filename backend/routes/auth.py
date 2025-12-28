"""
Authentication Routes for ACA DataHub
REST API endpoints for user auth, registration, 2FA, and session management
"""

from fastapi import APIRouter, HTTPException, Request, Depends, Header
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime

from services.auth_service import auth_service

router = APIRouter(prefix="/auth", tags=["Authentication"])


# =========================================================================
# Request/Response Models
# =========================================================================

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    totp_code: Optional[str] = None


class RefreshRequest(BaseModel):
    refresh_token: str


class PasswordResetRequest(BaseModel):
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str = Field(..., min_length=8)


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=8)


class Setup2FARequest(BaseModel):
    pass  # No body needed


class Enable2FARequest(BaseModel):
    secret: str
    code: str


class Disable2FARequest(BaseModel):
    password: str


class UpdateProfileRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    avatar_url: Optional[str] = None
    preferences: Optional[dict] = None


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int


class UserResponse(BaseModel):
    id: str
    email: str
    first_name: Optional[str]
    last_name: Optional[str]
    role: str
    is_verified: bool
    two_factor_enabled: bool
    created_at: str


# =========================================================================
# Helper Functions
# =========================================================================

def get_client_info(request: Request) -> tuple:
    """Extract client IP and user agent from request"""
    ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent", "")
    return ip, user_agent


async def get_current_user(
    request: Request,
    authorization: str = Header(None)
) -> dict:
    """Dependency to get current authenticated user from JWT"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization header")
    
    token = authorization.replace("Bearer ", "")
    payload = auth_service.decode_token(token)
    
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token type")
    
    return payload


# =========================================================================
# Public Auth Endpoints
# =========================================================================

@router.post("/register", response_model=dict)
async def register(data: RegisterRequest, request: Request):
    """
    Register a new user account.
    Returns user info and sends verification email.
    """
    try:
        result = auth_service.register_user(
            email=data.email,
            password=data.password,
            first_name=data.first_name,
            last_name=data.last_name
        )
        
        # In production, send verification email here
        # send_verification_email(result["user"]["email"], result["verification_token"])
        
        return {
            "message": "Registration successful. Please verify your email.",
            "user": result["user"]
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/login", response_model=dict)
async def login(data: LoginRequest, request: Request):
    """
    Authenticate user and return JWT tokens.
    If 2FA is enabled, may require totp_code.
    """
    ip, user_agent = get_client_info(request)
    
    try:
        result = auth_service.login(
            email=data.email,
            password=data.password,
            totp_code=data.totp_code,
            ip_address=ip,
            user_agent=user_agent
        )
        
        # Check if 2FA is required
        if result.get("requires_2fa"):
            return {
                "requires_2fa": True,
                "message": "Please provide your 2FA code"
            }
        
        return {
            "access_token": result["access_token"],
            "refresh_token": result["refresh_token"],
            "token_type": "bearer",
            "expires_in": result["expires_in"],
            "user": result["user"]
        }
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(data: RefreshRequest):
    """
    Refresh an expired access token using a valid refresh token.
    """
    try:
        result = auth_service.refresh_access_token(data.refresh_token)
        return TokenResponse(
            access_token=result["access_token"],
            token_type=result["token_type"],
            expires_in=result["expires_in"]
        )
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/logout")
async def logout(
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Invalidate the current session/token.
    """
    auth_service.logout(user["sub"])
    return {"message": "Logged out successfully"}


@router.post("/logout-all")
async def logout_all_sessions(
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Invalidate all sessions for the current user.
    """
    auth_service.logout(user["sub"])
    return {"message": "All sessions terminated"}


# =========================================================================
# Email Verification & Password Reset
# =========================================================================

@router.post("/verify-email")
async def verify_email(token: str):
    """
    Verify email address using the token from verification email.
    """
    try:
        auth_service.verify_email(token)
        return {"message": "Email verified successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/forgot-password")
async def forgot_password(data: PasswordResetRequest):
    """
    Request a password reset email.
    Always returns success to prevent email enumeration.
    """
    token = auth_service.request_password_reset(data.email)
    
    if token:
        # In production, send reset email here
        # send_password_reset_email(data.email, token)
        pass
    
    return {
        "message": "If an account exists with this email, a reset link has been sent."
    }


@router.post("/reset-password")
async def reset_password(data: PasswordResetConfirm):
    """
    Reset password using the token from reset email.
    """
    try:
        auth_service.reset_password(data.token, data.new_password)
        return {"message": "Password reset successful. Please login with your new password."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# =========================================================================
# Two-Factor Authentication
# =========================================================================

@router.post("/2fa/setup")
async def setup_2fa(
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Generate 2FA secret and QR code URI for authenticator app setup.
    """
    result = auth_service.setup_2fa(user["sub"])
    return {
        "secret": result["secret"],
        "provisioning_uri": result["provisioning_uri"],
        "message": "Scan the QR code with your authenticator app, then verify with a code"
    }


@router.post("/2fa/enable")
async def enable_2fa(
    data: Enable2FARequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Verify code and enable 2FA for the account.
    """
    try:
        auth_service.enable_2fa(user["sub"], data.secret, data.code)
        return {"message": "Two-factor authentication enabled successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/2fa/disable")
async def disable_2fa(
    data: Disable2FARequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Disable 2FA for the account (requires password confirmation).
    """
    try:
        auth_service.disable_2fa(user["sub"], data.password)
        return {"message": "Two-factor authentication disabled"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# =========================================================================
# User Profile & Session Management
# =========================================================================

@router.get("/me")
async def get_current_user_info(
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Get current authenticated user's profile.
    """
    user_data = auth_service.get_user_by_id(user["sub"])
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    return user_data


@router.put("/me")
async def update_profile(
    data: UpdateProfileRequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Update current user's profile information.
    """
    try:
        updates = data.dict(exclude_unset=True)
        updated_user = auth_service.update_user(user["sub"], updates)
        return updated_user
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/me/change-password")
async def change_password(
    data: ChangePasswordRequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Change current user's password.
    """
    try:
        auth_service.change_password(
            user["sub"],
            data.current_password,
            data.new_password
        )
        return {"message": "Password changed successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/me/sessions")
async def get_sessions(
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Get all active sessions for the current user.
    """
    sessions = auth_service.get_user_sessions(user["sub"])
    return {"sessions": sessions}


@router.delete("/me/sessions/{session_id}")
async def revoke_session(
    session_id: str,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Revoke a specific session.
    """
    auth_service.logout(user["sub"], session_id)
    return {"message": "Session revoked"}
