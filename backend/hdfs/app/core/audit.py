import os
import json
import logging
from typing import Any, Dict, Optional

# Реэкспорт и использование единого аудита из общего пакета
from backend.common.core.audit import (
    audit_log as _common_audit_log,
    log_audit_event,
    recent_audit_events,
    AuditEventType,
)

logger = logging.getLogger("hdfs_explorer.audit")
AUDIT_LOG_FILE = os.environ.get("AUDIT_LOG_FILE", "")


def audit_log(
    action: str,
    username: str,
    client_ip: str,
    details: Optional[Dict[str, Any]] = None,
    status: str = "SUCCESS",
):
    _common_audit_log(
        action=action,
        username=username,
        client_ip=client_ip,
        details=details,
        status=status,
    )
