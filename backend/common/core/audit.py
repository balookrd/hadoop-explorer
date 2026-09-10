import os
import json
import logging
import datetime
from collections import deque
from typing import Optional, Dict, Any

logger = logging.getLogger("hadoop_explorer.audit")

recent_audit_events: deque = deque(maxlen=500)


class AuditEventType:
    AUTH_LOGIN_SUCCESS = "AUTH_LOGIN_SUCCESS"
    AUTH_LOGIN_FAILED = "AUTH_LOGIN_FAILED"
    AUTH_RATE_LIMITED = "AUTH_RATE_LIMITED"
    AUTH_LOGOUT = "AUTH_LOGOUT"
    TOKEN_REVOKED = "TOKEN_REVOKED"
    CSRF_REJECTED = "CSRF_REJECTED"
    ACCESS_DENIED_ACL = "ACCESS_DENIED_ACL"
    ACCESS_DENIED_BOLA = "ACCESS_DENIED_BOLA"
    QUERY_EXECUTED = "QUERY_EXECUTED"
    QUERY_CANCELLED = "QUERY_CANCELLED"
    FILE_READ = "FILE_READ"
    FILE_WRITE = "FILE_WRITE"
    FILE_DELETE = "FILE_DELETE"
    CHANGE_REQUEST_CREATED = "CHANGE_REQUEST_CREATED"
    CHANGE_REQUEST_APPROVED = "CHANGE_REQUEST_APPROVED"
    CONFIG_APPLIED = "CONFIG_APPLIED"


AUDIT_LOG_FILE = ""


def audit_log(
    action: str,
    username: str,
    client_ip: str,
    details: Optional[Dict[str, Any]] = None,
    status: str = "SUCCESS",
    audit_file_override: Optional[str] = None,
):
    """
    Записывает структурированное событие аудита безопасности в JSON.
    """
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
        from backend.common.api.request_id_middleware import get_request_id

        req_id = get_request_id()
    except Exception:
        req_id = ""

    event = {
        "timestamp": now_iso,
        "action": action,
        "event_type": action,
        "username": username or "anonymous",
        "client_ip": client_ip or "unknown",
        "request_id": req_id or "unknown",
        "status": status,
        "details": details or {},
    }
    recent_audit_events.append(event)
    log_line = json.dumps(event, ensure_ascii=False)

    if status == "SUCCESS":
        logger.info(f"[AUDIT] {log_line}")
    elif status == "WARNING":
        logger.warning(f"[AUDIT] {log_line}")
    else:
        logger.error(f"[AUDIT] {log_line}")

    target_file = audit_file_override or AUDIT_LOG_FILE or os.environ.get("AUDIT_LOG_FILE", "")
    if target_file:
        try:
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            with open(target_file, "a", encoding="utf-8") as f:
                f.write(log_line + "\n")
        except Exception as e:
            logger.warning(f"Не удалось записать в audit log файл {target_file}: {e}")


def log_audit_event(
    event_type: str, username: str, client_ip: str, status: str = "SUCCESS", details: Optional[Dict[str, Any]] = None
):
    audit_log(action=event_type, username=username, client_ip=client_ip, details=details, status=status)
