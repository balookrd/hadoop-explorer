from __future__ import annotations
import os
import yaml
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, model_validator

class MockUser(BaseModel):
    username: str
    password: str
    display_name: str
    email: str
    groups: List[str] = []

class LDAPConfig(BaseModel):
    enabled: bool = True
    server_uri: str = "ldaps://ad.company.local:636"
    use_ssl: bool = True
    bind_dn: str = ""
    bind_password: str = ""
    user_base_dn: str = ""
    user_filter: str = "(&(objectClass=user)(sAMAccountName={username}))"
    user_display_name_attr: str = "displayName"
    user_email_attr: str = "mail"
    group_base_dn: str = ""
    group_filter: str = "(&(objectClass=group)(member={user_dn}))"
    group_name_attr: str = "cn"
    ca_cert_file: Optional[str] = None
    allow_insecure_ssl: bool = False

class KerberosConfig(BaseModel):
    enabled: bool = True
    keytab_file: Optional[str] = None
    service_principal: Optional[str] = None

class JWTConfig(BaseModel):
    secret_key: str = "secret-key-for-dev-only"
    algorithm: str = "HS256"
    expire_minutes: int = 480

class AuthConfig(BaseModel):
    mode: str = "mock"  # hybrid, ldaps_only, kerberos_only, mock
    mock_users: List[MockUser] = []
    ldap: LDAPConfig = Field(default_factory=LDAPConfig)
    kerberos: KerberosConfig = Field(default_factory=KerberosConfig)
    jwt: JWTConfig = Field(default_factory=JWTConfig)

class ClusterAclConfig(BaseModel):
    allowed_groups: List[str] = ["*"]
    allowed_users: List[str] = []

class ImpersonationConfig(BaseModel):
    enabled: bool = True
    method: str = "x-trino-user"  # x-trino-user, doAs

class ClusterConfig(BaseModel):
    id: str
    name: str
    type: str  # trino, hive, mock
    host: str = "localhost"
    port: int = 8443
    use_ssl: bool = False
    catalog: Optional[str] = None
    schema_: Optional[str] = Field(default=None, alias="schema")
    auth: Dict[str, Any] = Field(default_factory=dict)
    impersonation: ImpersonationConfig = Field(default_factory=ImpersonationConfig)
    allow_dml_ddl: bool = False
    acl: ClusterAclConfig = Field(default_factory=ClusterAclConfig)

    model_config = {"populate_by_name": True}

class UIAclConfig(BaseModel):
    allowed_users: List[str] = ["*"]
    allowed_groups: List[str] = ["*"]
    admin_groups: List[str] = []

class ACLConfig(BaseModel):
    ui_access: UIAclConfig = Field(default_factory=UIAclConfig)

class QueryDefaultsConfig(BaseModel):
    max_rows_in_ui: int = 10000
    default_limit: int = 1000
    auto_add_limit: bool = True
    query_timeout_seconds: int = 600

class AIConfig(BaseModel):
    enabled: bool = True
    provider: str = "openai_compatible"  # openai_compatible, ollama, mock
    base_url: str = "http://localhost:11434/v1"
    api_key: str = "ollama"
    model: str = "qwen2.5-coder:7b"
    timeout_seconds: int = 30
    temperature: float = 0.1
    max_tokens: int = 2048

class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    cors_origins: List[str] = ["http://localhost:8000", "http://localhost:5173"]
    secure_cookies: bool = False

class DatabaseConfig(BaseModel):
    url: str = "sqlite+aiosqlite:///./data/sql_explorer.db"

class AppConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    acl: ACLConfig = Field(default_factory=ACLConfig)
    clusters: List[ClusterConfig] = Field(default_factory=list)
    query_defaults: QueryDefaultsConfig = Field(default_factory=QueryDefaultsConfig)
    ai: AIConfig = Field(default_factory=AIConfig)

    @model_validator(mode="after")
    def validate_production_security(self) -> "AppConfig":
        if not self.server.debug:
            if self.auth.mode == "mock" or (self.auth.mode != "kerberos_only" and not self.auth.ldap.enabled):
                raise ValueError(
                    "Mock authentication cannot be used in production mode (debug=False). "
                    "Please configure LDAP/Kerberos or set server.debug=True for local development."
                )
            insecure_defaults = (
                "change-this-to-a-very-secret-random-key-in-production",
                "secret-key-for-dev-only",
                "default-secret-key-change-it"
            )
            if self.auth.jwt.secret_key in insecure_defaults or len(self.auth.jwt.secret_key) < 32:
                raise ValueError(
                    "JWT_SECRET_KEY must be set to a secure unique string (at least 32 characters) in production mode."
                )
        return self

def load_config(config_path: Optional[str] = None) -> AppConfig:
    if not config_path:
        env_cfg = os.getenv("CONFIG_PATH")
        if env_cfg:
            config_path = env_cfg
        else:
            from pathlib import Path
            project_cfg = Path(__file__).resolve().parents[3] / "config" / "config.yaml"
            if project_cfg.exists():
                config_path = str(project_cfg)
            else:
                config_path = "config/config.yaml"
    
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            raw_data = yaml.safe_load(f) or {}
            cfg = AppConfig(**raw_data)
    else:
        cfg = AppConfig()

    # Переопределение из переменных окружения (12-Factor App / Kubernetes Secrets)
    if os.getenv("SERVER_DEBUG") is not None:
        cfg.server.debug = os.environ["SERVER_DEBUG"].lower() in ("true", "1", "yes")
    elif os.getenv("DEBUG") is not None:
        cfg.server.debug = os.environ["DEBUG"].lower() in ("true", "1", "yes")

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
    if os.getenv("SECURE_COOKIES"):
        cfg.server.secure_cookies = os.environ["SECURE_COOKIES"].lower() in ("true", "1", "yes")
    if os.getenv("LDAP_ALLOW_INSECURE_SSL"):
        cfg.auth.ldap.allow_insecure_ssl = os.environ["LDAP_ALLOW_INSECURE_SSL"].lower() in ("true", "1", "yes")
    if os.getenv("AI_ENABLED"):
        cfg.ai.enabled = os.environ["AI_ENABLED"].lower() in ("true", "1", "yes")
    if os.getenv("AI_PROVIDER"):
        cfg.ai.provider = os.environ["AI_PROVIDER"]
    if os.getenv("AI_BASE_URL"):
        cfg.ai.base_url = os.environ["AI_BASE_URL"]
    if os.getenv("AI_API_KEY"):
        cfg.ai.api_key = os.environ["AI_API_KEY"]
    if os.getenv("AI_MODEL"):
        cfg.ai.model = os.environ["AI_MODEL"]

    return cfg.validate_production_security()

settings = load_config()
