from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Response, Request, status
import anyio

from app.core.config import settings
from app.core.security import (
    create_access_token,
    get_current_user,
    get_client_ip,
    UserSession
)
from app.core.audit import log_audit_event, AuditEventType
from app.core.ldap_auth import authenticate_ldap

router = APIRouter(prefix="/auth", tags=["auth"])

class LoginRequest(BaseModel):
    username: str
    password: str

class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserSession

@router.post("/login", response_model=AuthResponse)
async def login(req: LoginRequest, request: Request, response: Response):
    client_ip = get_client_ip(request)
    user_info = None

    if settings.auth.mode == "mock":
        for m in settings.auth.mock_users:
            if m.username == req.username and m.password == req.password:
                user_info = {
                    "username": m.username,
                    "display_name": m.display_name,
                    "email": m.email,
                    "groups": m.groups,
                    "auth_method": "mock"
                }
                break
    elif settings.auth.mode in ("hybrid", "ldaps_only") and settings.auth.ldap.enabled:
        user_info = await anyio.to_thread.run_sync(authenticate_ldap, req.username, req.password, settings.auth.ldap)

    if not user_info:
        log_audit_event(
            AuditEventType.AUTH_LOGIN_FAILED,
            username=req.username,
            client_ip=client_ip,
            status="FAILED"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверное имя пользователя или пароль"
        )

    is_admin = bool(set(user_info.get("groups", [])) & set(settings.acl.ui_access.admin_groups))
    user_info["is_admin"] = is_admin

    token = create_access_token(user_info)
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=settings.server.secure_cookies,
        samesite="lax",
        max_age=settings.auth.jwt.expire_minutes * 60
    )

    user_session = UserSession(
        username=user_info["username"],
        display_name=user_info["display_name"],
        email=user_info.get("email"),
        groups=user_info.get("groups", []),
        is_admin=is_admin,
        auth_method=user_info.get("auth_method", "password")
    )

    log_audit_event(
        AuditEventType.AUTH_LOGIN_SUCCESS,
        username=user_session.username,
        client_ip=client_ip,
        status="SUCCESS"
    )

    return AuthResponse(access_token=token, token_type="bearer", user=user_session)

@router.get("/me", response_model=UserSession)
async def get_me(current_user: UserSession = Depends(get_current_user)):
    return current_user

@router.post("/logout")
async def logout(response: Response):
    response.delete_cookie(key="session_token")
    return {"status": "ok", "message": "Успешный выход"}
