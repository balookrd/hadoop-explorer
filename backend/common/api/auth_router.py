import inspect
import logging
import secrets
from typing import Optional, Callable, Any, List
import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from backend.common.core.security import (
    create_jwt_token,
    decode_jwt_token,
    extract_token_from_request,
    verify_csrf as common_verify_csrf,
)
from backend.common.core.rate_limiter import auth_rate_limiter as default_rate_limiter, get_client_ip
from backend.common.core.audit import audit_log, AuditEventType
from backend.common.core.kerberos import kerberos_manager as default_kerberos_manager
from backend.common.models.auth import (
    LoginRequest,
    TokenResponse,
    UserSession,
    UserInfo,
    Role,
    resolve_system_role,
)


logger = logging.getLogger("hadoop_explorer.auth")
security_scheme = HTTPBearer(auto_error=False)


def create_auth_router(
    *,
    settings_provider: Callable[[], Any],
    storage_service: Any,
    get_current_user_dep: Callable,
    authenticate_mock_fn: Optional[Callable[[str, str], Optional[Any]]] = None,
    authenticate_ldap_fn: Optional[Callable[[str, str], Optional[Any]]] = None,
    get_ldap_user_info_fn: Optional[Callable[[str], Optional[Any]]] = None,
    acl_checker_fn: Optional[Callable[[Any], bool]] = None,
    rate_limiter: Optional[Any] = None,
    kerberos_authenticator: Optional[Callable[[str], Optional[Any]]] = None,
    cookie_name: str = "access_token",
    additional_cookie_names: Optional[List[str]] = None,
    on_logout_fn: Optional[Callable[[str], Any]] = None,
    prefix: str = "/api/v1/auth",
    tags: Optional[List[str]] = None,
) -> APIRouter:
    """
    Фабрика централизованного роутера аутентификации.
    Реализует /login, /sso (SPNEGO Kerberos), /me, /logout с полной поддержкой
    Rate Limiting, sliding window session, CSRF защиты и HttpOnly Cookies.
    """
    router = APIRouter(prefix=prefix, tags=tags or ["auth"])
    all_cookie_names = [cookie_name] + (additional_cookie_names or [])
    if rate_limiter:
        active_rate_limiter = rate_limiter
    else:
        from backend.common.core.rate_limiter import RateLimiter

        active_rate_limiter = RateLimiter(storage_getter=lambda: storage_service)

    def _get_settings():
        return settings_provider()

    def _get_jwt_config():
        settings = _get_settings()
        auth_cfg = getattr(settings, "auth", None)
        sec_cfg = getattr(settings, "security", None)

        secret_key = (
            getattr(getattr(auth_cfg, "jwt", None), "secret_key", None)
            or getattr(sec_cfg, "jwt_secret_key", None)
            or getattr(settings, "jwt_secret_key", None)
            or "dev-secret-key-change-in-production"
        )
        algorithm = (
            getattr(getattr(auth_cfg, "jwt", None), "algorithm", None)
            or getattr(sec_cfg, "jwt_algorithm", None)
            or getattr(settings, "jwt_algorithm", None)
            or "HS256"
        )
        expire_minutes = (
            getattr(getattr(auth_cfg, "jwt", None), "expire_minutes", None)
            or getattr(sec_cfg, "access_token_expire_minutes", None)
            or 480
        )
        return secret_key, algorithm, expire_minutes

    def _extract_mock_user(username: str, password: str) -> Optional[UserSession]:
        settings = _get_settings()
        if authenticate_mock_fn:
            res = authenticate_mock_fn(username, password)
            if res:
                if isinstance(res, UserSession):
                    role, is_adm = resolve_system_role(res.username, res.groups, settings)
                    if is_adm or res.is_admin:
                        res.is_admin = True
                        res.system_role = Role.ADMIN
                    elif not res.system_role or res.system_role == Role.READER:
                        res.system_role = role
                    return res
                if isinstance(res, UserInfo):
                    role, is_adm = resolve_system_role(res.username, res.groups, settings)
                    return UserSession(
                        username=res.username,
                        display_name=res.display_name,
                        email=res.email,
                        groups=res.groups,
                        auth_method="mock",
                        is_admin=res.is_admin or is_adm,
                        system_role=Role.ADMIN if (res.is_admin or is_adm) else role,
                    )
                if isinstance(res, dict):
                    role, is_adm = resolve_system_role(res.get("username", username), res.get("groups", []), settings)
                    res.setdefault("is_admin", is_adm)
                    res.setdefault("system_role", role)
                    return UserSession(**res)

        mock_users = getattr(getattr(settings, "auth", None), "mock_users", None) or []
        for m in mock_users:
            m_username = getattr(m, "username", "")
            m_password = getattr(m, "password", "")
            m_hash = getattr(m, "password_hash", None)
            if secrets.compare_digest(m_username, username):
                valid = False
                if m_hash:
                    try:
                        import bcrypt

                        valid = bcrypt.checkpw(password.encode("utf-8"), m_hash.encode("utf-8"))
                    except Exception:
                        valid = False
                elif m_password:
                    valid = secrets.compare_digest(m_password, password)

                if valid:
                    groups = getattr(m, "groups", [])
                    role, is_adm = resolve_system_role(m_username, groups, settings)
                    return UserSession(
                        username=m_username,
                        display_name=getattr(m, "display_name", m_username),
                        email=getattr(m, "email", None),
                        groups=groups,
                        auth_method="mock",
                        is_admin=is_adm,
                        system_role=role,
                    )
        return None

    @router.post("/login", response_model=TokenResponse)
    async def login(req: LoginRequest, request: Request, response: Response):
        client_ip = get_client_ip(request)
        active_rate_limiter.check_limit(f"{client_ip}:{req.username}", request)

        settings = _get_settings()
        auth_cfg = getattr(settings, "auth", None)
        mode = getattr(auth_cfg, "mode", None) or (
            "mock" if not getattr(getattr(settings, "ldap", None), "enabled", False) else "ldaps_only"
        )
        ldap_enabled = getattr(getattr(settings, "ldap", None), "enabled", False) or (
            auth_cfg and getattr(getattr(auth_cfg, "ldap", None), "enabled", False)
        )

        user_session = None
        if mode == "mock":
            user_session = _extract_mock_user(req.username, req.password)
        elif mode in ("hybrid", "ldaps_only") and ldap_enabled:
            if authenticate_ldap_fn:
                ldap_res = await anyio.to_thread.run_sync(authenticate_ldap_fn, req.username, req.password)
                if ldap_res:
                    if isinstance(ldap_res, UserSession):
                        user_session = ldap_res
                    elif isinstance(ldap_res, UserInfo):
                        role, is_adm = resolve_system_role(ldap_res.username, ldap_res.groups, settings)
                        user_session = UserSession(
                            username=ldap_res.username,
                            display_name=ldap_res.display_name,
                            email=ldap_res.email,
                            groups=ldap_res.groups,
                            auth_method="ldap",
                            is_admin=ldap_res.is_admin or is_adm,
                            system_role=Role.ADMIN if (ldap_res.is_admin or is_adm) else role,
                        )
                    elif isinstance(ldap_res, dict):
                        role, is_adm = resolve_system_role(
                            ldap_res.get("username", req.username), ldap_res.get("groups", []), settings
                        )
                        ldap_res.setdefault("is_admin", is_adm)
                        ldap_res.setdefault("system_role", role)
                        user_session = UserSession(**ldap_res)

        if not user_session:
            try:
                from backend.common.core.metrics import metrics_registry

                metrics_registry.auth_attempts_total.inc(app="hadoop-common", provider=mode, status="failure")
            except Exception:
                pass
            audit_log(AuditEventType.AUTH_LOGIN_FAILED, req.username, client_ip, status="FAILURE")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Неверное имя пользователя или пароль",
            )

        role, is_adm = resolve_system_role(user_session.username, user_session.groups, settings)
        if is_adm or user_session.is_admin:
            user_session.is_admin = True
            user_session.system_role = Role.ADMIN
        elif user_session.system_role == Role.READER and role == Role.WRITER:
            user_session.system_role = Role.WRITER

        if acl_checker_fn and not acl_checker_fn(user_session):
            audit_log(AuditEventType.ACCESS_DENIED_ACL, user_session.username, client_ip, status="DENIED")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Доступ к веб-интерфейсу запрещен политикой безопасности (ACL)",
            )

        secret_key, algorithm, expire_minutes = _get_jwt_config()

        token_payload = {
            "sub": user_session.username,
            "display_name": user_session.display_name,
            "email": user_session.email,
            "groups": user_session.groups,
            "auth_method": user_session.auth_method,
            "is_admin": user_session.is_admin,
            "system_role": user_session.system_role.value
            if hasattr(user_session.system_role, "value")
            else str(user_session.system_role),
            "user": user_session.model_dump(),
        }

        token = create_jwt_token(
            token_payload, secret_key=secret_key, algorithm=algorithm, expires_minutes=expire_minutes
        )
        payload = decode_jwt_token(token, secret_key=secret_key, algorithms=[algorithm])

        # Сохранение в SessionStore
        storage_service.save_session(
            token=token,
            user=user_session,
            expires_at=payload.get("exp") if payload else (expire_minutes * 60),
            jti=payload.get("jti") if payload else None,
        )

        try:
            from backend.common.core.metrics import metrics_registry

            metrics_registry.auth_attempts_total.inc(
                app="hadoop-common",
                provider=user_session.auth_method or "password",
                status="success",
            )
        except Exception:
            pass

        audit_log(AuditEventType.AUTH_LOGIN_SUCCESS, user_session.username, client_ip, status="SUCCESS")

        # Выставление Cookie
        secure = getattr(getattr(settings, "server", None), "secure_cookies", False) or getattr(
            getattr(settings, "security", None), "cookie_secure", False
        )
        samesite = getattr(getattr(settings, "security", None), "cookie_samesite", "lax")
        for c_name in all_cookie_names:
            response.set_cookie(
                key=c_name,
                value=token,
                httponly=True,
                secure=secure,
                samesite=samesite,
                max_age=expire_minutes * 60,
                path="/",
            )

        return TokenResponse(
            access_token=token,
            token_type="bearer",
            user=user_session,
            success=True,
            message="Авторизация успешна",
        )

    @router.get("/me", response_model=UserSession)
    async def get_me(
        request: Request,
        response: Response,
        current_user: Any = Depends(get_current_user_dep),
    ):
        secret_key, algorithm, expire_minutes = _get_jwt_config()
        extend_sec = expire_minutes * 60

        token, is_cookie_auth = extract_token_from_request(request, all_cookie_names)
        if token:
            storage_service.touch_session(token, extend_seconds=extend_sec)
            if is_cookie_auth:
                settings = _get_settings()
                secure = getattr(getattr(settings, "server", None), "secure_cookies", False) or getattr(
                    getattr(settings, "security", None), "cookie_secure", False
                )
                samesite = getattr(getattr(settings, "security", None), "cookie_samesite", "lax")
                for c_name in all_cookie_names:
                    if c_name in request.cookies:
                        response.set_cookie(
                            key=c_name,
                            value=token,
                            httponly=True,
                            secure=secure,
                            samesite=samesite,
                            max_age=extend_sec,
                            path="/",
                        )

        settings = _get_settings()
        if isinstance(current_user, UserSession):
            role, is_adm = resolve_system_role(current_user.username, current_user.groups, settings)
            if is_adm or current_user.is_admin:
                current_user.is_admin = True
                current_user.system_role = Role.ADMIN
            elif current_user.system_role == Role.READER and role == Role.WRITER:
                current_user.system_role = Role.WRITER
            return current_user

        u_dict = current_user if isinstance(current_user, dict) else current_user.model_dump()
        username = u_dict.get("username", "")
        groups = u_dict.get("groups", [])
        role, is_adm = resolve_system_role(username, groups, settings)
        is_admin_final = bool(u_dict.get("is_admin", False) or is_adm)
        system_role_final = Role.ADMIN if is_admin_final else (u_dict.get("system_role") or role)

        return UserSession(
            username=username,
            display_name=u_dict.get("display_name", username),
            email=u_dict.get("email"),
            groups=groups,
            auth_method=u_dict.get("auth_method", "jwt"),
            is_admin=is_admin_final,
            system_role=system_role_final,
        )

    @router.get("/sso", response_model=TokenResponse)
    async def kerberos_sso(request: Request, response: Response):
        client_ip = get_client_ip(request)
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Negotiate "):
            response.headers["WWW-Authenticate"] = "Negotiate"
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Требуется Kerberos аутентификация",
                headers={"WWW-Authenticate": "Negotiate"},
            )

        username = None
        out_token = None

        if kerberos_authenticator:
            auth_res = kerberos_authenticator(auth_header)
            if isinstance(auth_res, str):
                username = auth_res
            elif isinstance(auth_res, dict):
                username = auth_res.get("username")
                out_token = auth_res.get("out_token")
            elif isinstance(auth_res, (UserInfo, UserSession)):
                username = auth_res.username
        else:
            settings = _get_settings()
            auth_cfg = getattr(settings, "auth", None)
            kerberos_cfg = getattr(auth_cfg, "kerberos", None) or getattr(settings, "kerberos_sso", None)
            service_principal = getattr(kerberos_cfg, "service_principal", None)
            keytab_file = getattr(kerberos_cfg, "keytab_file", None) or getattr(kerberos_cfg, "keytab_path", None)

            spnego_res = default_kerberos_manager.authenticate_spnego(
                auth_header, service_principal=service_principal, keytab_path=keytab_file
            )
            if spnego_res:
                username = spnego_res["username"]
                out_token = spnego_res.get("out_token")

        if not username:
            audit_log("SPNEGO_FAILED", "unknown", client_ip, status="FAILURE")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Недействительный Kerberos токен")

        display_name = username
        email = f"{username}@company.local"
        groups = []

        settings = _get_settings()
        auth_cfg = getattr(settings, "auth", None)
        ldap_enabled = getattr(getattr(settings, "ldap", None), "enabled", False) or (
            auth_cfg and getattr(getattr(auth_cfg, "ldap", None), "enabled", False)
        )
        if ldap_enabled and get_ldap_user_info_fn:
            ldap_info = get_ldap_user_info_fn(username)
            if ldap_info:
                if isinstance(ldap_info, (UserInfo, UserSession)):
                    groups = ldap_info.groups
                    display_name = ldap_info.display_name or username
                    email = ldap_info.email or email
                elif isinstance(ldap_info, dict):
                    groups = ldap_info.get("groups", groups)
                    display_name = ldap_info.get("display_name", display_name)
                    email = ldap_info.get("email", email)

        role, is_adm = resolve_system_role(username, groups, settings)
        user_session = UserSession(
            username=username,
            display_name=display_name,
            email=email,
            groups=groups,
            auth_method="kerberos",
            is_admin=is_adm,
            system_role=role,
        )

        if acl_checker_fn and not acl_checker_fn(user_session):
            audit_log(AuditEventType.ACCESS_DENIED_ACL, user_session.username, client_ip, status="DENIED")
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Доступ запрещен политикой безопасности")

        secret_key, algorithm, expire_minutes = _get_jwt_config()

        token_payload = {
            "sub": user_session.username,
            "display_name": user_session.display_name,
            "email": user_session.email,
            "groups": user_session.groups,
            "auth_method": "kerberos",
            "is_admin": user_session.is_admin,
            "system_role": user_session.system_role.value
            if hasattr(user_session.system_role, "value")
            else str(user_session.system_role),
            "user": user_session.model_dump(),
        }

        token = create_jwt_token(
            token_payload, secret_key=secret_key, algorithm=algorithm, expires_minutes=expire_minutes
        )
        payload = decode_jwt_token(token, secret_key=secret_key, algorithms=[algorithm])

        storage_service.save_session(
            token=token,
            user=user_session,
            expires_at=payload.get("exp") if payload else (expire_minutes * 60),
            jti=payload.get("jti") if payload else None,
        )

        audit_log(AuditEventType.AUTH_LOGIN_SUCCESS, user_session.username, client_ip, status="SUCCESS")

        secure = getattr(getattr(settings, "server", None), "secure_cookies", False) or getattr(
            getattr(settings, "security", None), "cookie_secure", False
        )
        samesite = getattr(getattr(settings, "security", None), "cookie_samesite", "lax")
        for c_name in all_cookie_names:
            response.set_cookie(
                key=c_name,
                value=token,
                httponly=True,
                secure=secure,
                samesite=samesite,
                max_age=expire_minutes * 60,
                path="/",
            )

        if out_token:
            response.headers["WWW-Authenticate"] = f"Negotiate {out_token}"

        return TokenResponse(
            access_token=token,
            token_type="bearer",
            user=user_session,
            success=True,
            message="Авторизация успешна",
        )

    @router.post("/logout")
    async def logout(
        request: Request,
        response: Response,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_scheme),
    ):
        client_ip = get_client_ip(request)
        token = None
        is_cookie_auth = False

        if credentials and credentials.credentials:
            token = credentials.credentials
        else:
            token, is_cookie_auth = extract_token_from_request(request, all_cookie_names)

        if is_cookie_auth:
            settings = _get_settings()
            cors = getattr(getattr(settings, "server", None), "cors_origins", [])
            common_verify_csrf(request, is_cookie_auth=True, allowed_cors=cors)

        username = "unknown"
        if token:
            secret_key, algorithm, _ = _get_jwt_config()
            try:
                payload = decode_jwt_token(token, secret_key=secret_key, algorithms=[algorithm])
                if payload:
                    username = payload.get("sub") or (payload.get("user") or {}).get("username", "unknown")
                    exp = payload.get("exp")
                    jti = payload.get("jti")
                    storage_service.delete_session(token)
                    storage_service.revoke_token(token_or_jti=token, username=username, expires_at=exp)
                    if jti:
                        storage_service.revoke_token(token_or_jti=jti, username=username, expires_at=exp)
                else:
                    storage_service.delete_session(token)
                    storage_service.revoke_token(token_or_jti=token)
            except Exception as e:
                logger.debug(f"Ошибка отзыва токена при logout: {e}")
                storage_service.delete_session(token)
                storage_service.revoke_token(token_or_jti=token)

        audit_log(AuditEventType.AUTH_LOGOUT, username, client_ip, status="SUCCESS")

        for c_name in all_cookie_names:
            response.delete_cookie(key=c_name, path="/")

        if username and username != "unknown" and on_logout_fn:
            try:
                res = on_logout_fn(username)
                if inspect.isawaitable(res):
                    await res
            except Exception as e:
                logger.warning(f"Ошибка в on_logout_fn для пользователя {username}: {e}")

        return {"success": True, "message": "Вы успешно вышли из системы"}

    @router.get("/jwks.json", tags=["auth"])
    async def get_jwks():
        """Возвращает набор публичных ключей JWKS (RFC 7517) для верификации асимметричных JWT токенов."""
        from backend.common.core.jwt_keys import global_jwt_key_manager

        return global_jwt_key_manager.get_jwks()

    return router
