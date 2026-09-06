import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, List
import jwt
from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.core.config import settings
from app.services.storage import storage_service
from app.models.auth import TokenPayload, UserInfo


bearer_scheme = HTTPBearer(auto_error=False)


def create_access_token(user: UserInfo) -> str:
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=settings.security.access_token_expire_minutes)
    jti = str(uuid.uuid4())
    payload = {
        "sub": user.username,
        "display_name": user.display_name,
        "email": user.email,
        "groups": user.groups,
        "is_admin": user.is_admin,
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
        "jti": jti,
    }
    return jwt.encode(payload, settings.security.secret_key, algorithm=settings.security.algorithm)


def decode_access_token(token: str) -> Optional[TokenPayload]:
    try:
        payload = jwt.decode(
            token,
            settings.security.secret_key,
            algorithms=[settings.security.algorithm]
        )
        return TokenPayload(
            sub=payload["sub"],
            display_name=payload.get("display_name", payload["sub"]),
            email=payload.get("email"),
            groups=payload.get("groups", []),
            exp=payload["exp"],
            jti=payload.get("jti")
        )
    except (jwt.PyJWTError, KeyError, ValueError):
        return None


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
                detail="CSRF проверка не пройдена: межсайтовый запрос заблокирован"
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

        # Если ни одно из условий не выполнено, блокируем запрос
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF проверка не пройдена: запрос отклонен политикой безопасности источника"
        )


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
