import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Response, Request, status
from app.core.config import settings
from app.core.ldap_auth import ldap_client
from app.core.kerberos import kerberos_manager
from app.services.storage import storage_service
from app.core.security import (
    create_access_token,
    decode_access_token,
    extract_token_from_request,
    get_current_user,
    get_current_user_optional,
)
from app.core.acl import can_access_ui
from app.core.audit import audit_log
from app.core.rate_limiter import auth_rate_limiter, get_client_ip
from app.models.auth import LoginRequest, LoginResponse, UserInfo

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
async def login(
    login_req: LoginRequest,
    request: Request,
    response: Response
):
    client_ip = get_client_ip(request)
    auth_rate_limiter.check_limit(f"{client_ip}:{login_req.username}", request)
    mode = getattr(settings, "auth", None) and settings.auth.mode or ("mock" if not settings.ldap.enabled else "ldaps_only")
    user = None
    if mode == "mock":
        user = ldap_client.authenticate_mock(login_req.username, login_req.password)
    elif mode in ("hybrid", "ldaps_only") and settings.ldap.enabled:
        user = await anyio.to_thread.run_sync(ldap_client.authenticate_ldap, login_req.username, login_req.password)

    if not user:
        audit_log("LOGIN_FAILED", login_req.username, client_ip, status="FAILURE")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверное имя пользователя или пароль"
        )

    # Проверка глобального ACL на доступ к приложению
    if not can_access_ui(user.username, user.groups):
        audit_log("ACCESS_DENIED_ACL", user.username, client_ip, status="DENIED")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ к веб-интерфейсу запрещен политикой безопасности (ACL)"
        )

    token = create_access_token(user)
    audit_log("LOGIN_SUCCESS", user.username, client_ip, status="SUCCESS")

    # Установка безопасной HTTP-Only Cookie
    response.set_cookie(
        key=settings.security.cookie_name,
        value=token,
        httponly=True,
        secure=settings.security.cookie_secure,
        samesite=settings.security.cookie_samesite,
        max_age=settings.security.access_token_expire_minutes * 60,
        path="/"
    )

    return LoginResponse(
        success=True,
        user=user,
        message="Авторизация успешна"
    )


@router.get("/me", response_model=UserInfo)
async def get_me(current_user: UserInfo = Depends(get_current_user)):
    return current_user


@router.get("/sso")
async def kerberos_sso(
    request: Request,
    response: Response
):
    """
    SPNEGO Kerberos SSO авторизация.
    """
    client_ip = get_client_ip(request)
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Negotiate "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется Kerberos аутентификация",
            headers={"WWW-Authenticate": "Negotiate"}
        )

    username = kerberos_manager.authenticate_spnego(auth_header)
    if not username:
        audit_log("SPNEGO_FAILED", "unknown", client_ip, status="FAILURE")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный Kerberos токен"
        )

    # Загружаем группы пользователя через LDAP или fallback конфигурацию
    user = ldap_client.get_user_info(username)
    if not user:
        user = UserInfo(
            username=username,
            display_name=username,
            groups=[]
        )

    if not can_access_ui(user.username, user.groups):
        audit_log("ACCESS_DENIED_ACL", user.username, client_ip, status="DENIED")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ запрещен политикой безопасности"
        )

    token = create_access_token(user)
    audit_log("SPNEGO_SUCCESS", user.username, client_ip, status="SUCCESS")
    response.set_cookie(
        key=settings.security.cookie_name,
        value=token,
        httponly=True,
        secure=settings.security.cookie_secure,
        samesite=settings.security.cookie_samesite,
        max_age=settings.security.access_token_expire_minutes * 60,
        path="/"
    )

    return LoginResponse(success=True, user=user)


@router.post("/logout")
async def logout(request: Request, response: Response):
    client_ip = get_client_ip(request)
    auth_header = request.headers.get("Authorization")
    is_cookie_auth = False
    token = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[len("Bearer "):].strip()
    elif settings.security.cookie_name in request.cookies:
        token = request.cookies.get(settings.security.cookie_name)
        is_cookie_auth = True

    if is_cookie_auth:
        from app.core.security import verify_csrf
        verify_csrf(request, is_cookie_auth=True)

    username = "unknown"
    if token:
        payload = decode_access_token(token)
        if payload and payload.jti:
            username = payload.sub
            storage_service.revoke_token(payload.jti, payload.exp)

    audit_log("LOGOUT", username, client_ip, status="SUCCESS")

    response.delete_cookie(
        key=settings.security.cookie_name,
        path="/"
    )
    return {"success": True, "message": "Вы успешно вышли из системы"}
