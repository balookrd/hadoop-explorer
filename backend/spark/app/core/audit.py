import os
import json
import logging
from typing import Optional, Dict, Any

from backend.common.core.audit import (
    recent_audit_events,
    AuditEventType as CommonAuditEventType,
    audit_log as _common_audit_log,
    log_audit_event as _common_log_audit_event,
)

logger = logging.getLogger("security.audit")


class AuditEventType(CommonAuditEventType):
    SPARK_SESSION_CREATED = "SPARK_SESSION_CREATED"
    SPARK_SESSION_STOPPED = "SPARK_SESSION_STOPPED"
    SPARK_CODE_EXECUTED = "SPARK_CODE_EXECUTED"
    ACCESS_DENIED_YARN_QUEUE = "ACCESS_DENIED_YARN_QUEUE"


def log_audit_event(
    event_type: str,
    username: str,
    client_ip: str,
    status: str = "SUCCESS",
    details: Optional[Dict[str, Any]] = None,
):
    _common_log_audit_event(
        event_type=event_type,
        username=username,
        client_ip=client_ip,
        status=status,
        details=details or {},
    )
