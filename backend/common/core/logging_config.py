import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional


class JSONFormatter(logging.Formatter):
    """
    Структурированный JSON-форматтер для промышленного сбора логов (ELK / OpenSearch / Loki).
    Автоматически извлекает Request ID из контекста корутины.
    """

    def __init__(self, service_name: str = "hadoop-explorer"):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        req_id = ""
        try:
            from backend.common.api.request_id_middleware import get_request_id

            req_id = get_request_id()
        except Exception:
            pass

        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": self.service_name,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": req_id or getattr(record, "request_id", "unknown"),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra_data"):
            log_data["extra"] = record.extra_data

        return json.dumps(log_data, ensure_ascii=False)


def setup_logging(
    service_name: str = "hadoop-explorer",
    env: Optional[str] = None,
    debug: bool = False,
) -> None:
    """
    Инициализирует централизованное логирование:
    - Production: структурированный вывод в формате JSON
    - Development: человекочитаемый форматированный текст
    """
    environment = env or os.environ.get("ENV") or os.environ.get("ENVIRONMENT") or "development"
    is_prod = environment.lower() in ("prod", "production")
    log_level = logging.DEBUG if debug else logging.INFO

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Очищаем существующие обработчики
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    if is_prod:
        console_handler.setFormatter(JSONFormatter(service_name=service_name))
    else:
        text_fmt = logging.Formatter("[%(asctime)s] [%(levelname)-7s] [%(name)s] %(message)s")
        console_handler.setFormatter(text_fmt)

    root_logger.addHandler(console_handler)
