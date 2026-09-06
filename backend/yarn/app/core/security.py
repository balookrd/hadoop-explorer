import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
import jwt
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.core.config import settings
from app.models.auth import UserSession, Role

security_scheme = HTTPBearer(auto_error=False)


from urllib.parse import urlparse

def _is_allowed_origin(url_str: str, request: Request, allowed_cors: list[str]) -> bool:
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


def verify_csrf(request: Request, is_cookie_auth: bool):
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

        origin = request.headers.get("Origin")
        if origin and _is_allowed_origin(origin, request, settings.server.cors_origins):
            return

        referer = request.headers.get("Referer")
        if referer and _is_allowed_origin(referer, request, settings.server.cors_origins):
            return

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF protection: запрос отклонен политикой безопасности источника"
        )


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=settings.auth.jwt.expire_minutes)
    jti = str(uuid.uuid4())
    to_encode.update({"exp": expire, "jti": jti})
    encoded_jwt = jwt.encode(to_encode, settings.auth.jwt.secret_key, algorithm=settings.auth.jwt.algorithm)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, settings.auth.jwt.secret_key, algorithms=[settings.auth.jwt.algorithm])
        jti = payload.get("jti")
        if jti:
            from app.services.storage import storage_service
            if storage_service.is_token_revoked(jti):
                return None
        return payload
    except jwt.PyJWTError:
        return None


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
