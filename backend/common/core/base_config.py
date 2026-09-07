import logging
import secrets
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


_base_config_logger = logging.getLogger("hadoop_explorer.security")


class BaseServerSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    cors_origins: List[str] = Field(
        default_factory=lambda: [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:8000",
            "http://127.0.0.1:8000",
        ]
    )
    secure_cookies: bool = False


# Известные небезопасные дефолтные ключи, которые нельзя использовать в production
INSECURE_DEFAULT_KEYS = frozenset(
    {
        "",
        "change-this-in-production-secret-key-32-chars-long",
        "dev-secret-key-for-local-testing-replace-in-prod",
        "spark-explorer-super-secret-jwt-key-for-dev-32chars",
        "secret-key-for-dev-only",
        "default-secret-key-change-it",
        "yarn-explorer-super-secret-key-change-in-production-random-hash",
        "change-this-to-a-very-secret-random-key-in-production",
    }
)


class BaseJwtSettings(BaseModel):
    secret_key: str = ""
    algorithm: str = "HS256"
    expire_minutes: int = 480

    @model_validator(mode="after")
    def _ensure_secret_key(self) -> "BaseJwtSettings":
        """
        Гарантирует наличие секретного ключа JWT:
        - Если ключ пустой или из списка небезопасных дефолтов — генерирует эфемерный ключ
          и логирует CRITICAL предупреждение. Эфемерный ключ потеряется при перезапуске,
          что обеспечивает fail-fast поведение (все сессии аннулируются).
        """
        if not self.secret_key or self.secret_key in INSECURE_DEFAULT_KEYS:
            ephemeral = secrets.token_urlsafe(48)
            _base_config_logger.critical(
                "ВНИМАНИЕ БЕЗОПАСНОСТИ: JWT secret_key не задан или является небезопасным дефолтом! "
                "Сгенерирован эфемерный ключ, который будет утерян при перезапуске сервиса. "
                "Все активные сессии станут невалидными при рестарте. "
                "Задайте стойкий ключ (>= 32 символа) через переменную окружения JWT_SECRET_KEY."
            )
            self.secret_key = ephemeral
        elif len(self.secret_key) < 32:
            _base_config_logger.warning(
                "JWT secret_key слишком короткий (< 32 символов). "
                "Рекомендуется использовать ключ длиной не менее 32 символов."
            )
        return self


from backend.common.core.ldap_auth import CommonLdapConfig, LdapConfig

# BaseLdapSettings алиас для обратной совместимости внутри base_config
BaseLdapSettings = CommonLdapConfig


class BaseKerberosSettings(BaseModel):
    enabled: bool = False
    service_principal: Optional[str] = None
    keytab_path: Optional[str] = None
    keytab_file: Optional[str] = None


class BaseDatabaseSettings(BaseModel):
    url: str = "sqlite:////tmp/hadoop_explorer.db"
    redis_url: Optional[str] = None
