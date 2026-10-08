"""Модуль аутентификации и авторизации (RBAC) Hadoop gRPC Replicator."""

from datetime import datetime, timedelta, timezone
import logging
from typing import List, Optional
from fastapi import Cookie, Depends, HTTPException, Header, Request, status
import jwt
from pydantic import BaseModel, Field

from backend.common.models.auth import Role, UserInfo, UserSession

logger = logging.getLogger("replicator.auth")

SECRET_KEY = "replicator-secret-key-for-dev-and-demo"
ALGORITHM = "HS256"
COOKIE_NAME = "replicator_session"

# Демо-пользователи в соответствии с общей платформой Hadoop Explorer
MOCK_USERS = {
    "admin_user": {
        "username": "admin_user",
        "password": "password123",
        "display_name": "Александр Админов",
        "email": "admin@example.local",
        "groups": ["hadoop-admins", "domain admins"],
        "is_admin": True,
        "system_role": Role.ADMIN,
    },
    "de_user": {
        "username": "de_user",
        "password": "password123",
        "display_name": "Иван Датаинженеров",
        "email": "de@example.local",
        "groups": ["data-engineers"],
        "is_admin": False,
        "system_role": Role.WRITER,
    },
    "analyst_user": {
        "username": "analyst_user",
        "password": "password123",
        "display_name": "Анна Аналитикова",
        "email": "analyst@example.local",
        "groups": ["analytics"],
        "is_admin": False,
        "system_role": Role.READER,
    },
}


class LoginRequest(BaseModel):
    username: str
    password: str


class AuthTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserSession
    success: bool = True
    message: str = "Аутентификация успешна"


def create_jwt_token(user: UserSession, expires_delta: Optional[timedelta] = None) -> str:
    """Создание подписанного JWT токена."""
    delta = expires_delta or timedelta(hours=12)
    payload = {
        "sub": user.username,
        "display_name": user.display_name,
        "email": user.email,
        "groups": user.groups,
        "is_admin": user.is_admin,
        "system_role": user.system_role.value if hasattr(user.system_role, "value") else str(user.system_role),
        "auth_method": user.auth_method,
        "exp": datetime.now(timezone.utc) + delta,
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_jwt_token(token: str) -> Optional[UserSession]:
    """Декодирование и верификация токена."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        role_str = payload.get("system_role", "reader")
        try:
            role = Role(role_str)
        except ValueError:
            role = Role.READER

        return UserSession(
            username=payload["sub"],
            display_name=payload.get("display_name", payload["sub"]),
            email=payload.get("email"),
            groups=payload.get("groups", []),
            is_admin=payload.get("is_admin", False),
            system_role=role,
            auth_method=payload.get("auth_method", "jwt"),
        )
    except Exception as e:
        logger.debug(f"Ошибка декодирования JWT: {e}")
        return None


def authenticate_user(username: str, password: str) -> Optional[UserSession]:
    """Проверка учетных данных пользователя."""
    u_data = MOCK_USERS.get(username)
    if u_data and u_data["password"] == password:
        return UserSession(
            username=u_data["username"],
            display_name=u_data["display_name"],
            email=u_data["email"],
            groups=u_data["groups"],
            is_admin=u_data["is_admin"],
            system_role=u_data["system_role"],
            auth_method="mock",
        )
    return None


async def get_current_user(
    request: Request,
    replicator_session: Optional[str] = Cookie(None),
    authorization: Optional[str] = Header(None),
) -> UserSession:
    """
    Dependency для получения текущего аутентифицированного пользователя.

    Извлекает токен из Cookie или Bearer Header.
    Если токен не передан (например, прямой запрос из тестов/скриптов),
    возвращает пользователя по умолчанию (admin_user) для сохранения обратной совместимости.
    """
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
    elif replicator_session:
        token = replicator_session

    if token:
        user = decode_jwt_token(token)
        if user:
            return user

    # Если запрошен явный заголовок X-Remote-User
    remote_user = request.headers.get("x-remote-user")
    if remote_user and remote_user in MOCK_USERS:
        u_data = MOCK_USERS[remote_user]
        return UserSession(
            username=u_data["username"],
            display_name=u_data["display_name"],
            email=u_data["email"],
            groups=u_data["groups"],
            is_admin=u_data["is_admin"],
            system_role=u_data["system_role"],
            auth_method="header",
        )

    # Дефолтный пользователь для автоматических тестов/API без авторизации
    default_u = MOCK_USERS["admin_user"]
    return UserSession(
        username=default_u["username"],
        display_name=default_u["display_name"],
        email=default_u["email"],
        groups=default_u["groups"],
        is_admin=default_u["is_admin"],
        system_role=default_u["system_role"],
        auth_method="default",
    )
