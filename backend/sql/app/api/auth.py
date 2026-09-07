import anyio.to_thread
from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Response, Request, status
from app.core.config import settings
from app.core.security import (
    create_access_token,
    decode_access_token,
    get_current_user,
    get_client_ip,
    UserSession,
    revoke_token_in_db,
    verify_csrf
)
from app.core.rate_limiter import auth_rate_limiter
from app.core.acl import check_ui_access
from app.core.ldap_auth import authenticate_ldap, get_ldap_user_info
from app.core.kerberos import authenticate_spnego
from app.core.audit import log_audit_event, AuditEventType

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
    rate_key = f"{client_ip}:{req.username}"
    auth_rate_limiter.check_limit(rate_key, request)

    user_info = None

    # 1. Проверяем mock_users ТОЛЬКО если режим аутентификации строго равен 'mock'
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

    # 2. В режимах hybrid и ldaps_only аутентификация выполняется через LDAP (mock-пользователи строго запрещены)
    elif settings.auth.mode in ("hybrid", "ldaps_only") and settings.auth.ldap.enabled:
        user_info = await anyio.to_thread.run_sync(authenticate_ldap, req.username, req.password)

    if not user_info:
        log_audit_event(
            AuditEventType.AUTH_LOGIN_FAILED,
            username=req.username,
            client_ip=client_ip,
            status="FAILED",
            details={"reason": "Неверное имя пользователя или пароль"}
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверное имя пользователя или пароль"
        )

    # Проверка ACL на доступ к Web-UI
    admin_groups = set(settings.acl.ui_access.admin_groups)
    is_admin = bool(set(user_info["groups"]) & admin_groups)

    session_user = UserSession(
        username=user_info["username"],
        display_name=user_info.get("display_name", user_info["username"]),
        email=user_info.get("email"),
        groups=user_info.get("groups", []),
        is_admin=is_admin,
        auth_method=user_info.get("auth_method", "ldap")
    )

    if not check_ui_access(session_user):
        log_audit_event(
            AuditEventType.ACCESS_DENIED_ACL,
            username=session_user.username,
            client_ip=client_ip,
            status="DENIED",
            details={"resource": "web_ui", "groups": session_user.groups}
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ к Web-UI запрещен политиками безопасности (ACL). Обратитесь к администратору."
        )

    log_audit_event(
        AuditEventType.AUTH_LOGIN_SUCCESS,
        username=session_user.username,
        client_ip=client_ip,
        status="SUCCESS",
        details={"auth_method": session_user.auth_method, "groups": session_user.groups}
    )

    # Создаем JWT
    token_data = {
        "sub": session_user.username,
        "display_name": session_user.display_name,
        "email": session_user.email,
        "groups": session_user.groups,
        "auth_method": session_user.auth_method
    }
    access_token = create_access_token(token_data)

    # Сохраняем активную сессию в базу данных для устойчивости к рестартам
    from app.services.storage import storage_service
    storage_service.save_session(
        token=access_token,
        user=session_user,
        expires_at=settings.auth.jwt.expire_minutes * 60
    )

    # Выставляем HttpOnly Cookie для удобной работы в браузере и EventSource
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=settings.server.secure_cookies,
        samesite="lax",
        max_age=settings.auth.jwt.expire_minutes * 60
    )

    return AuthResponse(access_token=access_token, user=session_user)

@router.get("/sso")
async def kerberos_negotiate(request: Request, response: Response):
    """
    Эндпоинт для Kerberos SPNEGO SSO.
    Браузер отправляет заголовок 'Authorization: Negotiate <ticket>'.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Negotiate "):
        # Просим браузер аутентифицироваться через SPNEGO
        response.headers["WWW-Authenticate"] = "Negotiate"
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется аутентификация Kerberos SPNEGO"
        )

    token_b64 = auth_header[len("Negotiate "):].strip()
    user_info = authenticate_spnego(token_b64)

    if not user_info:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Ошибка валидации Kerberos билета"
        )

    groups = user_info.get("groups", [])
    display_name = user_info.get("display_name", user_info["username"])
    email = user_info.get("email")

    # Если включен LDAP, обогащаем группы и профиль пользователя из каталога
    if settings.auth.ldap.enabled:
        ldap_info = get_ldap_user_info(user_info["username"])
        if ldap_info:
            groups = ldap_info.get("groups", groups)
            if ldap_info.get("display_name"):
                display_name = ldap_info.get("display_name")
            if ldap_info.get("email"):
                email = ldap_info.get("email")

    admin_groups = set(settings.acl.ui_access.admin_groups)
    is_admin = bool(set(groups) & admin_groups)

    session_user = UserSession(
        username=user_info["username"],
        display_name=display_name,
        email=email,
        groups=groups,
        is_admin=is_admin,
        auth_method="kerberos"
    )

    if not check_ui_access(session_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ запрещен политиками ACL"
        )

    access_token = create_access_token({
        "sub": session_user.username,
        "display_name": session_user.display_name,
        "email": session_user.email,
        "groups": session_user.groups,
        "auth_method": "kerberos"
    })

    from app.services.storage import storage_service
    storage_service.save_session(
        token=access_token,
        user=session_user,
        expires_at=settings.auth.jwt.expire_minutes * 60
    )

    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=settings.server.secure_cookies,
        samesite="lax",
        max_age=settings.auth.jwt.expire_minutes * 60
    )

    if user_info.get("out_token"):
        response.headers["WWW-Authenticate"] = f"Negotiate {user_info['out_token']}"

    client_ip = get_client_ip(request)
    log_audit_event(
        AuditEventType.AUTH_LOGIN_SUCCESS,
        username=session_user.username,
        client_ip=client_ip,
        status="SUCCESS",
        details={"auth_method": "kerberos", "groups": session_user.groups}
    )

    return {"access_token": access_token, "user": session_user}

@router.get("/me", response_model=UserSession)
async def get_me(current_user: UserSession = Depends(get_current_user)):
    return current_user

@router.post("/logout")
async def logout(request: Request, response: Response):
    auth_header = request.headers.get("Authorization")
    token = None
    is_cookie_auth = False

    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()
    elif "access_token" in request.cookies:
        token = request.cookies.get("access_token")
        is_cookie_auth = True

    if is_cookie_auth:
        verify_csrf(request, is_cookie_auth=True)

    client_ip = get_client_ip(request)
    if token:
        payload = decode_access_token(token)
        username = payload.get("sub", "unknown") if payload else "unknown"
        from app.services.storage import storage_service
        storage_service.delete_session(token)
        await revoke_token_in_db(token, username=username)
        log_audit_event(
            AuditEventType.AUTH_LOGOUT,
            username=username,
            client_ip=client_ip,
            status="SUCCESS"
        )

    response.delete_cookie("access_token")
    return {"status": "ok", "message": "Успешный выход из системы"}

