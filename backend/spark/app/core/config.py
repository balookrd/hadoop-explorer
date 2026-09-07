from __future__ import annotations
import os
from pathlib import Path
from typing import List, Optional, Dict, Any
import yaml
from pydantic import BaseModel, Field, model_validator

from backend.common.core.ldap_auth import CommonLdapConfig


class MockUser(BaseModel):
    username: str
    password: str
    display_name: str
    email: str
    groups: List[str] = []


class KerberosConfig(BaseModel):
    enabled: bool = True
    keytab_file: Optional[str] = None
    service_principal: Optional[str] = None


class JWTConfig(BaseModel):
    secret_key: str = ""
    algorithm: str = "HS256"
    expire_minutes: int = 480


class AuthConfig(BaseModel):
    mode: str = "mock"  # mock, hybrid, ldaps_only, kerberos_only
    mock_users: List[MockUser] = []
    ldap: CommonLdapConfig = Field(default_factory=CommonLdapConfig)
    kerberos: KerberosConfig = Field(default_factory=KerberosConfig)
    jwt: JWTConfig = Field(default_factory=JWTConfig)


class UIAclConfig(BaseModel):
    allowed_users: List[str] = ["*"]
    allowed_groups: List[str] = ["*"]
    admin_groups: List[str] = ["hadoop-admins", "platform-admins"]


class ACLConfig(BaseModel):
    ui_access: UIAclConfig = Field(default_factory=UIAclConfig)


class PythonEnvConfig(BaseModel):
    id: str
    name: str
    python_path: str = "/usr/bin/python3"
    archive_path: Optional[str] = None
    is_default: bool = False


class SparkVersionConfig(BaseModel):
    id: str
    name: str
    spark_archive: Optional[str] = None
    livy_url: Optional[str] = None
    is_default: bool = False
    python_versions: List[PythonEnvConfig] = Field(default_factory=list)


class MetastoreConfig(BaseModel):
    id: str
    name: str
    uris: str
    is_default: bool = False
    spark_conf: Dict[str, str] = Field(default_factory=dict)


class ResourceProfileConfig(BaseModel):
    name: str
    driver_memory: str = "2g"
    driver_cores: int = 1
    executor_memory: str = "4g"
    executor_cores: int = 2
    num_executors: int = 2


class YarnQueueAcl(BaseModel):
    allowed_groups: List[str] = ["*"]
    allowed_users: List[str] = []


class YarnConfig(BaseModel):
    cluster_id: Optional[str] = None
    resource_manager_urls: List[str] = Field(default_factory=list)
    default_queue: str = "default"
    allowed_queues: List[str] = Field(default_factory=lambda: ["default"])
    queue_acl: Dict[str, YarnQueueAcl] = Field(default_factory=dict)


class ClusterAclConfig(BaseModel):
    allowed_groups: List[str] = ["*"]
    allowed_users: List[str] = []


class ImpersonationConfig(BaseModel):
    enabled: bool = True
    method: str = "proxyUser"  # proxyUser, doAs


class SparkClusterConfig(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    type: str = "spark"  # spark, mock
    livy_url: str = "http://localhost:8998"
    use_ssl: bool = False
    auth: Dict[str, Any] = Field(default_factory=dict)
    impersonation: ImpersonationConfig = Field(default_factory=ImpersonationConfig)
    acl: ClusterAclConfig = Field(default_factory=ClusterAclConfig)

    spark_versions: List[SparkVersionConfig] = Field(default_factory=list)
    default_repositories: List[str] = Field(default_factory=list)
    yarn: YarnConfig = Field(default_factory=YarnConfig)
    metastores: List[MetastoreConfig] = Field(default_factory=list)
    resource_profiles: Dict[str, ResourceProfileConfig] = Field(default_factory=dict)

    session_idle_timeout_seconds: int = 1800
    max_sessions_per_user: int = 3


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    cors_origins: List[str] = ["http://localhost:8000", "http://localhost:5173", "http://127.0.0.1:5173"]
    secure_cookies: bool = False


class DatabaseConfig(BaseModel):
    url: str = "sqlite+aiosqlite:///./data/spark_explorer.db"


class AppConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    acl: ACLConfig = Field(default_factory=ACLConfig)
    clusters: List[SparkClusterConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_production_security(self) -> "AppConfig":
        if not self.server.debug:
            if self.auth.mode == "mock" or (self.auth.mode != "kerberos_only" and not self.auth.ldap.enabled):
                raise ValueError(
                    "Mock authentication cannot be used in production mode (debug=False). "
                    "Configure LDAP/Kerberos or set server.debug=True for dev."
                )
        return self


def load_config(config_path: Optional[str] = None) -> AppConfig:
    if not config_path:
        env_cfg = os.getenv("CONFIG_PATH")
        if env_cfg:
            config_path = env_cfg
        else:
            cand = Path(__file__).resolve().parents[2] / "config" / "config.yaml"
            if cand.exists():
                config_path = str(cand)
            else:
                config_path = "config/config.yaml"

    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            raw_data = yaml.safe_load(f) or {}
            cfg = AppConfig(**raw_data)
    else:
        cfg = AppConfig()

    # Переопределения ENV
    if os.getenv("SERVER_DEBUG") is not None:
        cfg.server.debug = os.environ["SERVER_DEBUG"].lower() in ("true", "1", "yes")
    if os.getenv("DATABASE_URL"):
        cfg.database.url = os.environ["DATABASE_URL"]
    if os.getenv("JWT_SECRET_KEY"):
        cfg.auth.jwt.secret_key = os.environ["JWT_SECRET_KEY"]
    if os.getenv("LDAP_BIND_PASSWORD"):
        cfg.auth.ldap.bind_password = os.environ["LDAP_BIND_PASSWORD"]
    if os.getenv("KRB5_KEYTAB"):
        cfg.auth.kerberos.keytab_file = os.environ["KRB5_KEYTAB"]
    if os.getenv("KRB5_PRINCIPAL"):
        cfg.auth.kerberos.service_principal = os.environ["KRB5_PRINCIPAL"]
    if os.getenv("CORS_ORIGINS"):
        cfg.server.cors_origins = [o.strip() for o in os.environ["CORS_ORIGINS"].split(",") if o.strip()]

    return cfg


settings = load_config()
