import os
import json
import logging
from typing import Any, Dict, Optional

from backend.common.core.audit import (
    recent_audit_events,
    AuditEventType,
    audit_log as _common_audit_log,
    log_audit_event,
)

logger = logging.getLogger("yarn_explorer.audit")
AUDIT_LOG_FILE = ""


def audit_log(
    action: str,
    username: str,
    client_ip: str,
    details: Optional[Dict[str, Any]] = None,
    status: str = "SUCCESS",
):
    target_file = AUDIT_LOG_FILE or os.environ.get("AUDIT_LOG_FILE", "")
    _common_audit_log(
        action=action,
        username=username,
        client_ip=client_ip,
        details=details,
        status=status,
        audit_file_override=target_file,
    )
