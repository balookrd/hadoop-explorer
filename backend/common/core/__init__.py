from backend.common.core.security import (
    hash_token,
    verify_csrf,
    extract_token_from_request,
    create_jwt_token,
    decode_jwt_token,
)
from backend.common.core.audit import (
    AuditEventType,
    audit_log,
    log_audit_event,
    recent_audit_events,
)
from backend.common.core.rate_limiter import (
    RateLimiter,
    get_client_ip,
    is_trusted_proxy,
)
from backend.common.core.cache import L1RevokedTokenCache
from backend.common.core.ldap_auth import CommonLdapAuthService
from backend.common.core.session_store import SessionStore
from backend.common.core.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    CircuitBreakerOpenException,
    circuit_breaker_registry,
)
from backend.common.core.shutdown import GracefulShutdownManager, shutdown_manager
from backend.common.core.lock import DistributedLock, distributed_lock, LockAcquireError

__all__ = [
    "hash_token",
    "verify_csrf",
    "extract_token_from_request",
    "create_jwt_token",
    "decode_jwt_token",
    "AuditEventType",
    "audit_log",
    "log_audit_event",
    "recent_audit_events",
    "RateLimiter",
    "get_client_ip",
    "is_trusted_proxy",
    "L1RevokedTokenCache",
    "CommonLdapAuthService",
    "SessionStore",
    "CircuitBreaker",
    "CircuitState",
    "CircuitBreakerOpenException",
    "circuit_breaker_registry",
    "GracefulShutdownManager",
    "shutdown_manager",
    "DistributedLock",
    "distributed_lock",
    "LockAcquireError",
]
