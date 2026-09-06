import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, List
import jwt
from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from backend.common.core.security import (
    verify_csrf as common_verify_csrf,
    hash_token,
    create_jwt_token,
    decode_jwt_token
)
from app.core.config import settings
from app.services.storage import storage_service
from app.models.auth import TokenPayload, UserInfo


bearer_scheme = HTTPBearer(auto_error=False)


def create_access_token(user: UserInfo) -> str:
    payload = {
        "sub": user.username,
        "display_name": user.display_name,
        "email": user.email,
        "groups": user.groups,
        "is_admin": user.is_admin,
    }
    return create_jwt_token(
        data=payload,
        secret_key=settings.security.secret_key,
        algorithm=settings.security.algorithm,
        expires_minutes=settings.security.access_token_expire_minutes
    )


def decode_access_token(token: str) -> Optional[TokenPayload]:
    payload = decode_jwt_token(
        token=token,
        secret_key=settings.security.secret_key,
        algorithms=[settings.security.algorithm]
    )
    if not payload or "sub" not in payload:
        return None

    return TokenPayload(
        sub=payload["sub"],
        display_name=payload.get("display_name", payload["sub"]),
        email=payload.get("email"),
        groups=payload.get("groups", []),
        exp=payload.get("exp", 0),
        jti=payload.get("jti")
    )


def verify_csrf(request: Request, is_cookie_auth: bool):
    """
    Защита от CSRF через унифицированный модуль backend.common.core.security.
    """
    common_verify_csrf(request, is_cookie_auth, allowed_cors=settings.server.cors_origins)


def extract_token_from_request(request: Request) -> Optional[str]:
    """
    Извлекает JWT-токен из Cookie или заголовка Authorization: Bearer.
    """
    token = request.cookies.get(settings.security.cookie_name)
    if not token:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[len("Bearer "):].strip()
    return token


async def get_current_user_optional(request: Request) -> Optional[UserInfo]:
    is_cookie_auth = False
    # 1. Попытка получить токен из Cookie
    token = request.cookies.get(settings.security.cookie_name)
    if token:
        is_cookie_auth = True
    else:
        # 2. Если в Cookie нет, проверяем заголовок Authorization: Bearer <token>
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[len("Bearer "):].strip()

    if not token:
        return None

    # Проверка CSRF для аутентификации по Cookie
    verify_csrf(request, is_cookie_auth)

    payload = decode_access_token(token)
    if not payload:
        return None

    # Проверка отзыва токена (CWE-613) через StorageService (Redis/PostgreSQL/SQLite)
    if payload.jti and storage_service.is_token_revoked(payload.jti):
        return None

    from app.core.acl import is_global_admin
    is_admin = is_global_admin(payload.sub, payload.groups)

    return UserInfo(
        username=payload.sub,
        display_name=payload.display_name,
        email=payload.email,
        groups=payload.groups,
        is_admin=is_admin
    )


async def get_current_user(request: Request) -> UserInfo:
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Необходима авторизация. Пожалуйста, войдите в систему.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user
