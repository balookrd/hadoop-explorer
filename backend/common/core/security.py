import os
import uuid
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Union
from urllib.parse import urlparse

import jwt
from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security_bearer = HTTPBearer(auto_error=False)


def hash_token(token: str) -> str:
    """Возвращает SHA-256 хэш строки токена."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _is_allowed_origin(url_str: str, request: Request, allowed_cors: List[str]) -> bool:
    if not url_str:
        return False
    try:
        parsed = urlparse(url_str)
        if not parsed.scheme or not parsed.netloc:
            return False
        if parsed.scheme.lower() not in ("http", "https"):
            return False

        target_origin = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}".rstrip("/")
        target_netloc = parsed.netloc.lower()

        for allowed in allowed_cors:
            if allowed == "*":
                return True
            p_allowed = urlparse(allowed)
            if p_allowed.netloc:
                if f"{p_allowed.scheme.lower()}://{p_allowed.netloc.lower()}".rstrip("/") == target_origin:
                    return True
            elif allowed.rstrip("/").lower() == target_origin:
                return True

        req_host = request.headers.get("host", "").lower()
        if req_host and target_netloc == req_host:
            return True

        base_netloc = request.base_url.netloc.lower()
        if base_netloc and target_netloc == base_netloc:
            return True

        base_url_str = str(request.base_url).rstrip("/").lower()
        if target_origin == base_url_str:
            return True

        return False
    except Exception:
        return False


def verify_csrf(request: Request, is_cookie_auth: bool, allowed_cors: Optional[List[str]] = None):
    """
    Защита от Cross-Site Request Forgery (CWE-352).
    Если запрос аутентифицирован через Cookie и изменяет состояние (POST, PUT, DELETE, PATCH),
    требуется подтверждение легитимности источника (Sec-Fetch-Site, Origin, Referer, X-Requested-With).
    """
    if not is_cookie_auth:
        return

    if request.method in ("POST", "PUT", "DELETE", "PATCH"):
        sec_fetch_site = request.headers.get("Sec-Fetch-Site")
        if sec_fetch_site and sec_fetch_site.lower() == "cross-site":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="CSRF protection: межсайтовый запрос отклонен (Sec-Fetch-Site: cross-site)"
            )

        x_requested_with = request.headers.get("X-Requested-With")
        if x_requested_with == "XMLHttpRequest":
            return

        cors_list = allowed_cors or []
        origin = request.headers.get("Origin")
        if origin and _is_allowed_origin(origin, request, cors_list):
            return

        referer = request.headers.get("Referer")
        if referer and _is_allowed_origin(referer, request, cors_list):
            return

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF protection: запрос отклонен политикой безопасности источника"
        )


def extract_token_from_request(request: Request, cookie_names: Optional[List[str]] = None) -> tuple[Optional[str], bool]:
    """
    Извлекает токен из Authorization: Bearer либо из Cookies.
    Возвращает кортеж: (token, is_cookie_auth).
    """
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[len("Bearer "):].strip()
        if token:
            return token, False

    cookies = cookie_names or ["access_token", "hdfs_explorer_session", "session_token"]
    for c_name in cookies:
        token = request.cookies.get(c_name)
        if token:
            return token, True

    return None, False


def create_jwt_token(
    data: dict,
    secret_key: str,
    algorithm: str = "HS256",
    expires_minutes: int = 480,
    expires_delta: Optional[timedelta] = None
) -> str:
    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=expires_minutes)

    jti = to_encode.get("jti") or uuid.uuid4().hex
    to_encode.update({
        "exp": int(expire.timestamp()),
        "iat": int(now.timestamp()),
        "jti": jti,
    })
    return jwt.encode(to_encode, secret_key, algorithm=algorithm)


def decode_jwt_token(token: str, secret_key: str, algorithms: Optional[List[str]] = None) -> Optional[dict]:
    try:
        payload = jwt.decode(
            token,
            secret_key,
            algorithms=algorithms or ["HS256"]
        )
        return payload
    except (jwt.PyJWTError, KeyError, ValueError):
        return None
