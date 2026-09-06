import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Request, Response
from fastapi.security import HTTPAuthorizationCredentials

from app.core.config import settings
from app.core.security import create_access_token, get_current_user, security_scheme
from app.core.ldap_auth import ldap_service
from app.core.kerberos import kerberos_manager
from app.core.acl import _check_match
from app.core.rate_limiter import auth_rate_limiter, get_client_ip
from app.models.auth import LoginRequest, UserSession, TokenResponse, Role

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


def _resolve_global_role(username: str, groups: list) -> Role:
    """Определяет глобальную роль пользователя."""
    user_groups = set(groups)
    g = settings.acl.roles
    if _check_match(username, user_groups, g.admin.users, g.admin.groups):
        return Role.ADMIN
    if _check_match(username, user_groups, g.writer.users, g.writer.groups):
        return Role.WRITER
    return Role.READER


def _mock_authenticate(username: str, password: str):
    """Аутентификация через mock-пользователей с поддержкой bcrypt и защитой от timing attacks."""
    for mock_user in settings.auth.mock_users:
        if secrets.compare_digest(mock_user.username, username):
            password_valid = False
            if mock_user.password_hash:
                import bcrypt
                try:
                    password_valid = bcrypt.checkpw(
                        password.encode("utf-8"),
                        mock_user.password_hash.encode("utf-8"),
                    )
                except Exception:
                    password_valid = False
            elif mock_user.password:
                password_valid = secrets.compare_digest(mock_user.password, password)

            if password_valid:
                role = _resolve_global_role(username, mock_user.groups)
                return UserSession(
                    username=mock_user.username,
                    display_name=mock_user.display_name,
                    email=mock_user.email,
                    groups=mock_user.groups,
                    auth_method="mock",
                    is_admin=(role == Role.ADMIN),
                    system_role=role,
                )
    return None


@router.post("/login", response_model=TokenResponse)
async def login(
    request: Request,
    body: LoginRequest,
    response: Response,
):
    """Эндпоинт авторизации: поддержка mock, LDAP, hybrid с rate limiting и HttpOnly cookie."""
    client_ip = get_client_ip(request)
    auth_rate_limiter.check_limit(f"{client_ip}:{body.username}", request)

    user = None
    mode = settings.auth.mode

    # Mock auth разрешен ТОЛЬКО в строгом режиме mode == "mock"
    if mode == "mock":
        user = _mock_authenticate(body.username, body.password)
    elif mode in ("ldaps_only", "hybrid") and settings.auth.ldap.enabled:
        # В режимах ldaps_only и hybrid mock-пользователи строго запрещены
        ldap_user = ldap_service.authenticate(body.username, body.password)
        if ldap_user:
            role = _resolve_global_role(ldap_user.username, ldap_user.groups)
            ldap_user.system_role = role
            ldap_user.is_admin = (role == Role.ADMIN)
            user = ldap_user

    client_ip = get_client_ip(request)
    if not user:
        from app.core.audit import audit_log
        audit_log(
            action="LOGIN_FAILED",
            username=body.username,
            client_ip=client_ip,
            details={"auth_mode": mode},
            status="WARNING",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверное имя пользователя или пароль",
        )

    token = create_access_token(
        data={"user": user.model_dump()},
        expires_delta=timedelta(minutes=settings.auth.jwt.expire_minutes),
    )

    from app.core.audit import audit_log
    audit_log(
        action="LOGIN_SUCCESS",
        username=user.username,
        client_ip=client_ip,
        details={"auth_method": user.auth_method, "role": user.system_role.value},
        status="SUCCESS",
    )

    # Установка безопасной HttpOnly cookie
    response.set_cookie(
        key="access_token",
        value=token,
        max_age=settings.auth.jwt.expire_minutes * 60,
        httponly=True,
        samesite="lax",
        secure=not settings.server.debug,
        path="/",
    )

    return TokenResponse(access_token=token, user=user)


@router.get("/sso", response_model=TokenResponse)
async def spnego_login(
    request: Request,
    response: Response,
):
    """Kerberos SPNEGO SSO авторизация с rate limiting и HttpOnly cookie."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Negotiate "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется заголовок Authorization: Negotiate <token>",
            headers={"WWW-Authenticate": "Negotiate"},
        )

    user = kerberos_manager.authenticate_spnego(auth_header)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Ошибка SPNEGO-аутентификации",
        )

    # Если включен LDAP, обогащаем профиль и список групп пользователя из каталога
    if settings.auth.ldap.enabled:
        ldap_user = ldap_service.get_user_info(user.username)
        if ldap_user:
            user.groups = ldap_user.groups
            user.display_name = ldap_user.display_name
            user.email = ldap_user.email

    role = _resolve_global_role(user.username, user.groups)
    user.system_role = role
    user.is_admin = (role == Role.ADMIN)

    client_ip = get_client_ip(request)
    from app.core.audit import audit_log
    audit_log(
        action="SPNEGO_LOGIN_SUCCESS",
        username=user.username,
        client_ip=client_ip,
        details={"auth_method": "kerberos", "role": user.system_role.value, "groups": user.groups},
        status="SUCCESS",
    )

    token = create_access_token(
        data={"user": user.model_dump()},
        expires_delta=timedelta(minutes=settings.auth.jwt.expire_minutes),
    )

    response.set_cookie(
        key="access_token",
        value=token,
        max_age=settings.auth.jwt.expire_minutes * 60,
        httponly=True,
        samesite="lax",
        secure=not settings.server.debug,
        path="/",
    )

    return TokenResponse(access_token=token, user=user)


@router.get("/me", response_model=UserSession)
async def get_me(user: UserSession = Depends(get_current_user)):
    """Возвращает текущего авторизованного пользователя."""
    return user


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_scheme),
):
    """Выход: отзыв токена на стороне сервера (blacklist) и удаление cookie."""
    token = None
    is_cookie_auth = False
    if credentials:
        token = credentials.credentials
    elif "access_token" in request.cookies:
        token = request.cookies.get("access_token")
        is_cookie_auth = True

    if is_cookie_auth:
        from app.core.security import verify_csrf
        verify_csrf(request, is_cookie_auth)

    if token:
        from app.services.storage import storage_service
        # Декодируем токен без проверки на отзыв, чтобы извлечь jti для отзыва
        try:
            import jwt
            payload = jwt.decode(
                token,
                settings.auth.jwt.secret_key,
                algorithms=[settings.auth.jwt.algorithm],
                options={"verify_exp": False},
            )
            jti = payload.get("jti")
            if jti:
                exp = payload.get("exp")
                if isinstance(exp, (int, float)):
                    exp_iso = datetime.fromtimestamp(exp, timezone.utc).isoformat()
                else:
                    exp_iso = datetime.now(timezone.utc).isoformat()
                storage_service.revoke_token(jti, exp_iso)
        except Exception as e:
            logger.debug(f"Ошибка при отзыве токена во время logout: {e}")

    response.delete_cookie(key="access_token", path="/")
    return {"detail": "Сессия успешно завершена и токен отозван"}
