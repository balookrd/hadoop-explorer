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

        for allowed in allowed_cors:
            if allowed == "*":
                return True
            p_allowed = urlparse(allowed)
            if p_allowed.netloc:
                if f"{p_allowed.scheme.lower()}://{p_allowed.netloc.lower()}".rstrip("/") == target_origin:
                    return True
            elif allowed.rstrip("/").lower() == target_origin:
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
                detail="CSRF protection: межсайтовый запрос отклонен (Sec-Fetch-Site: cross-site)",
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
            detail="CSRF protection: запрос отклонен политикой безопасности источника",
        )


CANONICAL_COOKIE_NAMES: List[str] = [
    "access_token",
    "hdfs_explorer_session",
    "session_token",
    "hadoop_explorer_session",
]


def extract_token_from_request(
    request: Request, cookie_names: Optional[List[str]] = None
) -> tuple[Optional[str], bool]:
    """
    Извлекает токен из Authorization: Bearer либо из Cookies.
    Возвращает кортеж: (token, is_cookie_auth).
    """
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[len("Bearer ") :].strip()
        if token:
            return token, False

    cookies = cookie_names or CANONICAL_COOKIE_NAMES
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
    expires_delta: Optional[timedelta] = None,
    kid: Optional[str] = None,
) -> str:
    from backend.common.core.jwt_keys import global_jwt_key_manager

    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=expires_minutes)

    jti = to_encode.get("jti") or uuid.uuid4().hex
    to_encode.update(
        {
            "exp": int(expire.timestamp()),
            "iat": int(now.timestamp()),
            "jti": jti,
        }
    )

    if algorithm.startswith("RS") or algorithm.startswith("ES"):
        # Если есть зарегистрированный приватный ключ в KeyManager
        active_priv_key = global_jwt_key_manager.get_active_private_key()
        if active_priv_key is not None:
            return global_jwt_key_manager.sign_jwt(to_encode)
        # Иначе используем secret_key как PEM-строку
        headers = {"kid": kid} if kid else None
        return jwt.encode(to_encode, secret_key, algorithm=algorithm, headers=headers)

    return jwt.encode(to_encode, secret_key, algorithm=algorithm)


def decode_jwt_token(token: str, secret_key: str, algorithms: Optional[List[str]] = None) -> Optional[dict]:
    from backend.common.core.jwt_keys import global_jwt_key_manager

    algs = algorithms or ["HS256", "RS256"]
    # 1. Пробуем декодировать через KeyManager (если токен содержит kid или подписан RS256)
    try:
        unverified_headers = jwt.get_unverified_header(token)
        alg = unverified_headers.get("alg", "HS256")
        if alg.startswith("RS") or alg.startswith("ES"):
            key_mgr_res = global_jwt_key_manager.verify_jwt(token)
            if key_mgr_res is not None:
                return key_mgr_res
    except Exception:
        pass

    # 2. Стандартная проверка по secret_key (симметричный ключ или PEM-публичный ключ)
    try:
        payload = jwt.decode(token, secret_key, algorithms=algs)
        return payload
    except (jwt.PyJWTError, KeyError, ValueError):
        return None


# ==============================================================================
# Общие абстракции для дедупликации security-логики между сервисами
# ==============================================================================

from pydantic import BaseModel
from typing import Callable, Any, Awaitable
from backend.common.models.auth import Role, resolve_system_role


class CommonUserSession(BaseModel):
    """
    Базовая модель сессии пользователя, общая для всех сервисов.
    Сервисы могут наследоваться от неё, добавляя специфические поля.
    """

    username: str
    display_name: str
    email: Optional[str] = None
    groups: List[str] = []
    is_admin: bool = False
    auth_method: str = "ldap"
    system_role: Role = Role.READER


def create_access_token(
    user_data: dict,
    secret_key: str,
    algorithm: str = "HS256",
    expire_minutes: int = 480,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """Удобная обёртка над create_jwt_token для сервисов."""
    return create_jwt_token(
        data=user_data,
        secret_key=secret_key,
        algorithm=algorithm,
        expires_minutes=expire_minutes,
        expires_delta=expires_delta,
    )


def decode_access_token(
    token: str,
    secret_key: str,
    algorithm: str = "HS256",
) -> Optional[dict]:
    """Удобная обёртка над decode_jwt_token для сервисов."""
    return decode_jwt_token(
        token=token,
        secret_key=secret_key,
        algorithms=[algorithm],
    )


def make_token_helpers(
    get_secret_key: Callable[[], str],
    get_algorithm: Callable[[], str],
    default_expire_minutes: int = 480,
) -> tuple[
    Callable[..., str],
    Callable[[str], Optional[dict]],
]:
    """
    Фабрика для генерации типизированных вспомогательных функций создания и декодирования
    JWT токенов с динамическим получением секретного ключа и алгоритма сервиса.
    Позволяет устранить дублирование однотипного boilerplate-кода в микросервисах.
    """

    def create_token(
        user_data: Optional[dict] = None,
        expires_minutes: Optional[int] = None,
        expires_delta: Optional[timedelta] = None,
        data: Optional[dict] = None,
    ) -> str:
        payload = user_data if user_data is not None else (data or {})
        return create_jwt_token(
            data=payload,
            secret_key=get_secret_key(),
            algorithm=get_algorithm(),
            expires_minutes=expires_minutes or default_expire_minutes,
            expires_delta=expires_delta,
        )

    def decode_token(token: str) -> Optional[dict]:
        return decode_jwt_token(
            token=token,
            secret_key=get_secret_key(),
            algorithms=[get_algorithm()],
        )

    return create_token, decode_token


# Тип для callback'а определения admin-прав
AdminResolver = Callable[[str, List[str], Optional[dict]], bool]


def make_get_current_user(
    get_secret_key: Callable[[], str],
    get_algorithm: Callable[[], str],
    get_cookie_names: Callable[[], List[str]],
    get_cors_origins: Callable[[], List[str]],
    get_storage_service: Callable[[], Any],
    admin_resolver: AdminResolver,
    user_session_class: type = CommonUserSession,
    extra_user_fields: Optional[Callable[[dict], dict]] = None,
):
    """
    Фабрика, создающая функции get_current_user и get_current_user_optional,
    сконфигурированные для конкретного сервиса.

    Args:
        get_secret_key: callback, возвращающий JWT secret key из настроек сервиса
        get_algorithm: callback, возвращающий JWT algorithm
        get_cookie_names: callback, возвращающий список имён cookies сервиса
        get_cors_origins: callback, возвращающий CORS origins для CSRF-проверки
        get_storage_service: callback, возвращающий storage_service экземпляр
        admin_resolver: callback(username, groups, session_data) -> bool для определения is_admin
        user_session_class: класс модели сессии (по умолчанию CommonUserSession)
        extra_user_fields: опциональный callback для извлечения дополнительных полей из payload/session
    """

    async def get_current_user_optional(request: Request) -> Optional[Any]:
        # 1. Извлекаем токен из запроса
        token, is_cookie_auth = extract_token_from_request(request, get_cookie_names())

        if not token:
            return None

        # 2. CSRF-проверка для Cookie-аутентификации
        verify_csrf(request, is_cookie_auth, allowed_cors=get_cors_origins())

        storage = get_storage_service()

        # 3. Проверка отзыва токена
        if storage.is_token_revoked(token):
            return None

        # 4. Проверка активной сессии в персистентной БД
        session_data = storage.get_session(token)
        if session_data:
            username = session_data["username"]
            groups = session_data.get("groups", [])
            calc_role, calc_adm = resolve_system_role(username, groups)
            is_admin = session_data.get("is_admin")
            if is_admin is None:
                is_admin = admin_resolver(username, groups, session_data) or calc_adm
            else:
                is_admin = bool(is_admin or calc_adm)

            raw_role = session_data.get("system_role")
            if is_admin:
                role = Role.ADMIN
            elif raw_role and raw_role != Role.READER:
                try:
                    role = Role(raw_role)
                except ValueError:
                    role = calc_role
            else:
                role = calc_role

            fields = {
                "username": username,
                "display_name": session_data.get("display_name", username),
                "email": session_data.get("email"),
                "groups": groups,
                "is_admin": bool(is_admin),
                "auth_method": session_data.get("auth_method", "ldap"),
            }
            if "system_role" in getattr(user_session_class, "model_fields", {}):
                fields["system_role"] = role
            if extra_user_fields:
                fields.update(extra_user_fields(session_data))
            return user_session_class(**fields)

        # 5. Fallback: декодирование JWT
        secret = get_secret_key()
        algo = get_algorithm()
        payload = decode_jwt_token(token, secret, algorithms=[algo])
        if not payload:
            return None

        # Проверка отзыва по JTI
        jti = payload.get("jti")
        if jti and storage.is_token_revoked(jti):
            return None

        # Извлечение данных пользователя из payload
        username = payload.get("sub") or payload.get("username")
        if not username:
            # Некоторые сервисы кладут user dict в payload["user"]
            user_dict = payload.get("user")
            if user_dict and isinstance(user_dict, dict):
                try:
                    u_groups = user_dict.get("groups", [])
                    c_role, c_adm = resolve_system_role(user_dict.get("username", ""), u_groups)
                    if c_adm:
                        user_dict["is_admin"] = True
                        user_dict["system_role"] = Role.ADMIN
                    elif "system_role" not in user_dict or user_dict["system_role"] == Role.READER:
                        user_dict["system_role"] = c_role
                    user = user_session_class(**user_dict)
                    # Сохраняем сессию
                    storage.save_session(
                        token=token,
                        user=user,
                        expires_at=payload.get("exp"),
                        jti=jti,
                    )
                    return user
                except Exception:
                    pass
            return None

        groups = payload.get("groups", [])
        calc_role, calc_adm = resolve_system_role(username, groups)
        is_admin = admin_resolver(username, groups, payload) or calc_adm or bool(payload.get("is_admin", False))

        raw_role = payload.get("system_role")
        if is_admin:
            role = Role.ADMIN
        elif raw_role and raw_role != Role.READER:
            try:
                role = Role(raw_role)
            except ValueError:
                role = calc_role
        else:
            role = calc_role

        fields = {
            "username": username,
            "display_name": payload.get("display_name", username),
            "email": payload.get("email"),
            "groups": groups,
            "is_admin": bool(is_admin),
            "auth_method": payload.get("auth_method", "jwt"),
        }
        if "system_role" in getattr(user_session_class, "model_fields", {}):
            fields["system_role"] = role
        if extra_user_fields:
            fields.update(extra_user_fields(payload))
        user = user_session_class(**fields)

        # Сохраняем сессию в БД для устойчивости к рестартам
        storage.save_session(
            token=token,
            user=user,
            expires_at=payload.get("exp"),
            jti=jti,
        )

        return user

    async def get_current_user(request: Request) -> Any:
        token, is_cookie_auth = extract_token_from_request(request, get_cookie_names())
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Требуется авторизация",
                headers={"WWW-Authenticate": "Bearer"},
            )

        verify_csrf(request, is_cookie_auth, allowed_cors=get_cors_origins())

        storage = get_storage_service()
        if storage.is_token_revoked(token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Токен отозван при выходе из системы",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user = await get_current_user_optional(request)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Недействительный или просроченный токен",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return user

    return get_current_user, get_current_user_optional


# ==============================================================================
# Content-Security-Policy (CSP) и стандартизация Security Headers
# ==============================================================================

CSP_DEFAULT_DIRECTIVES = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline'; "
    "style-src 'self' 'unsafe-inline'; "
    "font-src 'self' data:; "
    "img-src 'self' data:; "
    "connect-src 'self'; "
    "frame-ancestors 'none'; "
    "object-src 'none'; "
    "base-uri 'self'; "
    "form-action 'self';"
)

# Примечание по безопасности (SEC-4):
# Для Monaco Editor и Web Workers требуются директивы 'unsafe-eval' и blob:
# (см. https://github.com/microsoft/monaco-editor/issues/2026).
# Во избежание ослабления защиты всего приложения эти послабления изолированы исключительно
# в CSP_CODE_EDITOR_DIRECTIVES для интерактивных редакторов кода (SQL Explorer / Spark Explorer).
# Для стандартных страниц всегда применяется жесткий CSP_DEFAULT_DIRECTIVES без eval.
CSP_CODE_EDITOR_DIRECTIVES = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob:; "
    "style-src 'self' 'unsafe-inline'; "
    "font-src 'self' data:; "
    "img-src 'self' data: blob:; "
    "connect-src 'self' ws: wss: http: https:; "
    "worker-src 'self' blob:; "
    "frame-ancestors 'none'; "
    "object-src 'none'; "
    "base-uri 'self'; "
    "form-action 'self';"
)


def apply_security_headers(
    response: Any,
    is_secure_cookie: bool = False,
    is_code_editor: bool = False,
    custom_csp: Optional[str] = None,
) -> Any:
    """
    Применяет единый набор защитных HTTP-заголовков безопасности к ответу FastAPI.
    """
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = custom_csp or (
        CSP_CODE_EDITOR_DIRECTIVES if is_code_editor else CSP_DEFAULT_DIRECTIVES
    )
    if is_secure_cookie:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response
