from typing import List, Optional
from pydantic import BaseModel, Field


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


class BaseJwtSettings(BaseModel):
    secret_key: str = "change-this-in-production-secret-key-32-chars-long"
    algorithm: str = "HS256"
    expire_minutes: int = 480


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
