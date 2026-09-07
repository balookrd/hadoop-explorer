from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Response, Request, status
import anyio

from app.core.config import settings
from app.core.security import (
    create_access_token,
    get_current_user,
    get_client_ip,
    UserSession,
)
from app.core.audit import log_audit_event, AuditEventType
from app.core.ldap_auth import authenticate_ldap
from app.services.storage import storage_service
from backend.common.core.security import decode_jwt_token

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
                    "auth_method": "mock",
                }
                break
    elif settings.auth.mode in ("hybrid", "ldaps_only") and settings.auth.ldap.enabled:
        user_info = await anyio.to_thread.run_sync(authenticate_ldap, req.username, req.password)

    if not user_info:
        log_audit_event(AuditEventType.AUTH_LOGIN_FAILED, username=req.username, client_ip=client_ip, status="FAILED")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Неверное имя пользователя или пароль")

    is_admin = bool(set(user_info.get("groups", [])) & set(settings.acl.ui_access.admin_groups))
    user_info["is_admin"] = is_admin

    token = create_access_token(user_info)
    payload = decode_jwt_token(
        token=token, secret_key=settings.auth.jwt.secret_key, algorithms=[settings.auth.jwt.algorithm]
    )

    user_session = UserSession(
        username=user_info["username"],
        display_name=user_info["display_name"],
        email=user_info.get("email"),
        groups=user_info.get("groups", []),
        is_admin=is_admin,
        auth_method=user_info.get("auth_method", "password"),
        token_jti=payload.get("jti") if payload else None,
    )

    # Персистентное сохранение сессии в SessionStore БД
    storage_service.save_session(
        token=token,
        user=user_session,
        expires_at=payload.get("exp") if payload else (settings.auth.jwt.expire_minutes * 60),
        jti=payload.get("jti") if payload else None,
    )

    # Установка безопасных HttpOnly Cookie
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=settings.server.secure_cookies,
        samesite="lax",
        max_age=settings.auth.jwt.expire_minutes * 60,
        path="/",
    )
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        secure=settings.server.secure_cookies,
        samesite="lax",
        max_age=settings.auth.jwt.expire_minutes * 60,
        path="/",
    )

    log_audit_event(
        AuditEventType.AUTH_LOGIN_SUCCESS, username=user_session.username, client_ip=client_ip, status="SUCCESS"
    )

    return AuthResponse(access_token=token, token_type="bearer", user=user_session)


@router.get("/me", response_model=UserSession)
async def get_me(current_user: UserSession = Depends(get_current_user)):
    return current_user


@router.post("/logout")
async def logout(request: Request, response: Response):
    token = None
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[len("Bearer ") :].strip()
    if not token:
        token = request.cookies.get("session_token") or request.cookies.get("access_token")

    if token:
        payload = decode_jwt_token(
            token=token, secret_key=settings.auth.jwt.secret_key, algorithms=[settings.auth.jwt.algorithm]
        )
        username = payload.get("sub", "unknown") if payload else "unknown"
        storage_service.revoke_token(token, username=username)

    response.delete_cookie(key="session_token", path="/")
    response.delete_cookie(key="access_token", path="/")
    return {"status": "ok", "message": "Успешный выход"}


@router.get("/sso", response_model=AuthResponse)
async def kerberos_sso(request: Request, response: Response):
    """
    Kerberos SPNEGO SSO авторизация через заголовок Authorization: Negotiate <ticket>.
    """
    client_ip = get_client_ip(request)
    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Negotiate "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется Kerberos аутентификация",
            headers={"WWW-Authenticate": "Negotiate"},
        )

    username = None
    in_token_b64 = auth_header[len("Negotiate ") :].strip()
    try:
        import spnego

        context = spnego.server(
            service="HTTP",
            hostname=settings.auth.kerberos.service_principal.split("/")[1].split("@")[0]
            if "/" in (settings.auth.kerberos.service_principal or "")
            else None,
            protocol="kerberos",
            keytab=settings.auth.kerberos.keytab_file if settings.auth.kerberos.keytab_file else None,
        )
        import base64

        in_token = base64.b64decode(in_token_b64)
        context.step(in_token)
        if context.complete:
            client_name = context.client_principal
            username = client_name.split("@")[0] if "@" in client_name else client_name
    except Exception:
        pass

    # В mock-режиме (для тестов и стендов) принимаем валидный mock negotiate
    if not username and (settings.server.debug or settings.auth.mode == "mock"):
        if in_token_b64 in ("mock_ticket", "dev_spnego_token"):
            username = "admin_user"

    if not username:
        log_audit_event(AuditEventType.AUTH_LOGIN_FAILED, username="unknown", client_ip=client_ip, status="FAILED")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Недействительный Kerberos токен")

    # Ищем пользователя в mock_users или LDAP
    user_info = {
        "username": username,
        "display_name": username,
        "email": f"{username}@company.local",
        "groups": ["hadoop-admins"],
        "auth_method": "kerberos",
    }
    for m in settings.auth.mock_users:
        if m.username == username:
            user_info["display_name"] = m.display_name
            user_info["email"] = m.email
            user_info["groups"] = m.groups
            break

    is_admin = bool(set(user_info.get("groups", [])) & set(settings.acl.ui_access.admin_groups))
    user_info["is_admin"] = is_admin

    token = create_access_token(user_info)
    payload = decode_jwt_token(
        token=token, secret_key=settings.auth.jwt.secret_key, algorithms=[settings.auth.jwt.algorithm]
    )

    user_session = UserSession(
        username=user_info["username"],
        display_name=user_info["display_name"],
        email=user_info.get("email"),
        groups=user_info.get("groups", []),
        is_admin=is_admin,
        auth_method="kerberos",
        token_jti=payload.get("jti") if payload else None,
    )

    storage_service.save_session(
        token=token,
        user=user_session,
        expires_at=payload.get("exp") if payload else (settings.auth.jwt.expire_minutes * 60),
        jti=payload.get("jti") if payload else None,
    )

    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=settings.server.secure_cookies,
        samesite="lax",
        max_age=settings.auth.jwt.expire_minutes * 60,
        path="/",
    )
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        secure=settings.server.secure_cookies,
        samesite="lax",
        max_age=settings.auth.jwt.expire_minutes * 60,
        path="/",
    )

    log_audit_event(AuditEventType.AUTH_LOGIN_SUCCESS, username=username, client_ip=client_ip, status="SUCCESS")

    return AuthResponse(access_token=token, token_type="bearer", user=user_session)
