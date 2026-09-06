import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
import jwt
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from backend.common.core.security import (
    verify_csrf as common_verify_csrf,
    hash_token,
    create_jwt_token,
    decode_jwt_token
)
from app.core.config import settings
from app.models.auth import UserSession, Role

security_scheme = HTTPBearer(auto_error=False)


def verify_csrf(request: Request, is_cookie_auth: bool):
    """
    Защита от Cross-Site Request Forgery (CWE-352) через унифицированный backend.common.core.security.
    """
    common_verify_csrf(request, is_cookie_auth, allowed_cors=settings.server.cors_origins)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    return create_jwt_token(
        data=data,
        secret_key=settings.auth.jwt.secret_key,
        algorithm=settings.auth.jwt.algorithm,
        expires_minutes=settings.auth.jwt.expire_minutes,
        expires_delta=expires_delta
    )


def decode_access_token(token: str) -> Optional[dict]:
    payload = decode_jwt_token(
        token=token,
        secret_key=settings.auth.jwt.secret_key,
        algorithms=[settings.auth.jwt.algorithm]
    )
    if not payload:
        return None

    jti = payload.get("jti")
    if jti:
        from app.services.storage import storage_service
        if storage_service.is_token_revoked(jti):
            return None
    return payload


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_scheme)
) -> UserSession:
    token = None
    is_cookie_auth = False
    if credentials:
        token = credentials.credentials
    elif "access_token" in request.cookies:
        token = request.cookies.get("access_token")
        is_cookie_auth = True

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется авторизация (отсутствует токен)",
            headers={"WWW-Authenticate": "Bearer"},
        )

    verify_csrf(request, is_cookie_auth)

    payload = decode_access_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный или истекший токен сессии",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_dict = payload.get("user")
    if not user_dict:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Некорректная структура токена пользователя",
        )

    user = UserSession(**user_dict)
    from app.core.acl import check_ui_access
    if not check_ui_access(user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ к системе запрещен политикой UI Access",
        )

    return user
