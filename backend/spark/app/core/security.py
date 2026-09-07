import os
import uuid
import datetime
import ipaddress
from typing import Optional, List
from pydantic import BaseModel
import jwt
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from backend.common.core.security import (
    verify_csrf as common_verify_csrf,
    hash_token,
    create_jwt_token,
    decode_jwt_token
)
from app.core.config import settings

security_bearer = HTTPBearer(auto_error=False)

def _get_trusted_proxies() -> set[str]:
    base = {"127.0.0.1", "::1", "localhost", "testclient"}
    env_p = os.getenv("TRUSTED_PROXIES", "")
    if env_p:
        base.update(p.strip() for p in env_p.split(",") if p.strip())
    return base

def get_client_ip(request: Request) -> str:
    client_host = request.client.host if request.client else "unknown"
    if client_host in _get_trusted_proxies():
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()
    return client_host

class UserSession(BaseModel):
    username: str
    display_name: str
    email: Optional[str] = None
    groups: List[str] = []
    is_admin: bool = False
    auth_method: str = "mock"
    token_jti: Optional[str] = None

def create_access_token(user_data: dict, expires_delta: Optional[datetime.timedelta] = None) -> str:
    to_encode = user_data.copy()
    expire = datetime.datetime.now(datetime.timezone.utc) + (
        expires_delta or datetime.timedelta(minutes=settings.auth.jwt.expire_minutes)
    )
    to_encode.update({"exp": expire, "jti": str(uuid.uuid4())})
    return create_jwt_token(
        data=to_encode,
        secret_key=settings.auth.jwt.secret_key,
        algorithm=settings.auth.jwt.algorithm,
        expires_delta=expires_delta or datetime.timedelta(minutes=settings.auth.jwt.expire_minutes)
    )

async def get_current_user(
    request: Request,
    auth_header: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer)
) -> UserSession:
    token = None
    if auth_header and auth_header.scheme.lower() == "bearer":
        token = auth_header.credentials
    if not token:
        token = request.cookies.get("session_token")
    if not token:
        token = request.query_params.get("token")

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Необходима авторизация",
            headers={"WWW-Authenticate": "Bearer"}
        )

    try:
        payload = decode_jwt_token(
            token=token,
            secret_key=settings.auth.jwt.secret_key,
            algorithms=[settings.auth.jwt.algorithm]
        )
        username: str = payload.get("sub") or payload.get("username")
        if not username:
            raise HTTPException(status_code=401, detail="Невалидный токен")

        groups: List[str] = payload.get("groups", [])
        is_admin = bool(set(groups) & set(settings.acl.ui_access.admin_groups))
        return UserSession(
            username=username,
            display_name=payload.get("display_name", username),
            email=payload.get("email"),
            groups=groups,
            is_admin=is_admin or payload.get("is_admin", False),
            auth_method=payload.get("auth_method", "jwt"),
            token_jti=payload.get("jti")
        )
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Токен недействителен или истек")

def require_admin(current_user: UserSession = Depends(get_current_user)) -> UserSession:
    if not current_user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Требуются права администратора")
    return current_user
