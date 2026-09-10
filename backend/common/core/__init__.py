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
from backend.common.core.session_store import SessionStore, StorageUnavailableException
from backend.common.core.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    CircuitBreakerOpenException,
    circuit_breaker_registry,
)
from backend.common.core.shutdown import GracefulShutdownManager, shutdown_manager
from backend.common.core.lock import DistributedLock, distributed_lock, LockAcquireError
from backend.common.core.retry import retry_async, with_retry
from backend.common.core.metrics import (
    metrics_registry,
    MetricsRegistry,
    PrometheusMetricsMiddleware,
    Counter,
    Gauge,
    Histogram,
)
from backend.common.core.logging_config import JSONFormatter, setup_logging
from backend.common.core.http_client import create_async_http_client
from backend.common.core.jwt_keys import JWTKeyManager, global_jwt_key_manager
from backend.common.core.tracing import (
    OpenTelemetryTracer,
    OpenTelemetryMiddleware,
    global_tracer,
    get_current_trace_id,
    get_current_span_id,
    Span,
)

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
    "StorageUnavailableException",
    "CircuitBreaker",
    "CircuitState",
    "CircuitBreakerOpenException",
    "circuit_breaker_registry",
    "GracefulShutdownManager",
    "shutdown_manager",
    "DistributedLock",
    "distributed_lock",
    "LockAcquireError",
    "KerberosManager",
    "kerberos_manager",
    "retry_async",
    "with_retry",
    "metrics_registry",
    "MetricsRegistry",
    "PrometheusMetricsMiddleware",
    "Counter",
    "Gauge",
    "Histogram",
    "create_async_http_client",
    "JSONFormatter",
    "setup_logging",
    "JWTKeyManager",
    "global_jwt_key_manager",
    "OpenTelemetryTracer",
    "OpenTelemetryMiddleware",
    "global_tracer",
    "get_current_trace_id",
    "get_current_span_id",
    "Span",
]
