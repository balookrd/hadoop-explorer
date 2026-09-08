import logging
import uuid
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError, HTTPException
from backend.common.core.circuit_breaker import CircuitBreakerOpenException

logger = logging.getLogger("hadoop_explorer.error_handlers")


def setup_global_exception_handlers(app: FastAPI):
    """
    Регистрирует централизованные обработчики исключений для FastAPI приложения:
    - Защищает от утечки внутреннего stack trace (CWE-209) при необработанных ошибках 500.
    - Стандартизирует ответы CircuitBreakerOpenException с кодом 503 и заголовком Retry-After.
    - Логирует инциденты со сгенерированным incident_id.
    """

    @app.exception_handler(CircuitBreakerOpenException)
    async def circuit_breaker_exception_handler(request: Request, exc: CircuitBreakerOpenException):
        logger.warning(f"CircuitBreaker [{exc.name}] OPEN на запросе {request.method} {request.url.path}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": "ServiceUnavailable",
                "message": str(exc),
                "retry_after": exc.retry_after,
            },
            headers={"Retry-After": str(int(exc.retry_after) + 1)},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "detail": exc.errors(),
            },
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        headers = getattr(exc, "headers", None)
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=headers,
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        incident_id = uuid.uuid4().hex[:8]
        logger.exception(
            f"Необработанное исключение (Incident ID: {incident_id}) на {request.method} {request.url.path}: {exc}"
        )
        try:
            from backend.common.core.metrics import metrics_registry

            app_name = getattr(app, "title", "hadoop-explorer")
            metrics_registry.exceptions_total.inc(
                app=app_name, exception_type=type(exc).__name__
            )
        except Exception:
            pass

        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "InternalServerError",
                "message": "Внутренняя ошибка сервера. Обратитесь к администратору.",
                "incident_id": incident_id,
            },
        )
