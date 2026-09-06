import os
import yaml
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings

from app.models.cluster import ClusterConfig
from app.models.auth import Role


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    cors_origins: List[str] = Field(
        default_factory=lambda: [
            "http://localhost:8000",
            "http://127.0.0.1:8000",
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ]
    )


class MockUserConfig(BaseModel):
    username: str
    password: Optional[str] = None
    password_hash: Optional[str] = None
    display_name: str
    email: Optional[str] = None
    groups: List[str] = Field(default_factory=list)


class LdapConfig(BaseModel):
    enabled: bool = False
    server_uri: str = "ldaps://localhost:636"
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
    verify_cert: bool = True
    allow_insecure_ssl: bool = False


class KerberosConfig(BaseModel):
    enabled: bool = False
    keytab_file: Optional[str] = None
    service_principal: Optional[str] = None


class JwtConfig(BaseModel):
    secret_key: str = "default-secret-key-change-it"
    algorithm: str = "HS256"
    expire_minutes: int = 480


class AuthConfig(BaseModel):
    mode: str = "mock"  # mock | ldaps_only | kerberos_only | hybrid
    mock_users: List[MockUserConfig] = Field(default_factory=list)
    ldap: LdapConfig = Field(default_factory=LdapConfig)
    kerberos: KerberosConfig = Field(default_factory=KerberosConfig)
    jwt: JwtConfig = Field(default_factory=JwtConfig)


class RoleMapping(BaseModel):
    users: List[str] = Field(default_factory=list)
    groups: List[str] = Field(default_factory=list)


class GlobalRoles(BaseModel):
    admin: RoleMapping = Field(default_factory=RoleMapping)
    writer: RoleMapping = Field(default_factory=RoleMapping)
    reader: RoleMapping = Field(default_factory=lambda: RoleMapping(users=["*"], groups=["*"]))


class UiAccessAcl(BaseModel):
    allowed_users: List[str] = Field(default_factory=lambda: ["*"])
    allowed_groups: List[str] = Field(default_factory=lambda: ["*"])


class AclConfig(BaseModel):
    ui_access: UiAccessAcl = Field(default_factory=UiAccessAcl)
    roles: GlobalRoles = Field(default_factory=GlobalRoles)


class DatabaseConfig(BaseModel):
    url: str = "sqlite:///data/yarn_explorer.db"


class Settings(BaseSettings):
    server: ServerConfig = Field(default_factory=ServerConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    acl: AclConfig = Field(default_factory=AclConfig)
    clusters: List[ClusterConfig] = Field(default_factory=list)

    def validate_production_security(self) -> "Settings":
        if not self.server.debug:
            if self.auth.mode == "mock":
                raise ValueError(
                    "Mock authentication cannot be used in production mode (debug=False). "
                    "Please configure LDAP or Kerberos SPNEGO authentication or set server.debug=True for local development."
                )
            insecure_defaults = (
                "default-secret-key-change-it",
                "yarn-explorer-super-secret-key-change-in-production-random-hash",
                "change-this-in-production-secret-key-32-chars-long"
            )
            if self.auth.jwt.secret_key in insecure_defaults or len(self.auth.jwt.secret_key) < 32:
                raise ValueError(
                    "JWT_SECRET_KEY must be set to a secure unique string (at least 32 characters) in production mode."
                )
        return self

    @classmethod
    def load_from_yaml(cls, config_path: Optional[str] = None) -> "Settings":
        if not config_path:
            config_path = os.environ.get("CONFIG_PATH")
            if not config_path:
                project_cfg = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "config", "config.yaml"))
                if os.path.exists(project_cfg):
                    config_path = project_cfg
                else:
                    config_path = "config/config.yaml"

        data = {}
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

        inst = cls(**data)

        # Переопределение секретов и параметров из переменных окружения
        env_auth_mode = os.environ.get("AUTH_MODE") or os.environ.get("YARN_AUTH_MODE")
        if env_auth_mode:
            inst.auth.mode = env_auth_mode
        env_db_url = os.environ.get("YARN_DATABASE_URL") or os.environ.get("DATABASE_URL")
        if env_db_url:
            inst.database.url = env_db_url
        elif os.environ.get("DB_PATH"):
            db_p = os.environ.get("DB_PATH")
            if db_p == ":memory:":
                inst.database.url = "sqlite:///:memory:"
            elif "://" in db_p:
                inst.database.url = db_p
            else:
                inst.database.url = f"sqlite:///{db_p}"

        env_jwt_secret = os.environ.get("JWT_SECRET_KEY")
        if env_jwt_secret:
            inst.auth.jwt.secret_key = env_jwt_secret

        env_ldap_password = os.environ.get("LDAP_BIND_PASSWORD")
        if env_ldap_password:
            inst.auth.ldap.bind_password = env_ldap_password

        env_debug = os.environ.get("SERVER_DEBUG") or os.environ.get("DEBUG")
        if env_debug is not None:
            inst.server.debug = env_debug.lower() in ("1", "true", "yes")

        env_cors = os.environ.get("CORS_ORIGINS")
        if env_cors:
            inst.server.cors_origins = [o.strip() for o in env_cors.split(",") if o.strip()]

        if inst.auth.jwt.secret_key in ("default-secret-key-change-it", "yarn-explorer-super-secret-key-change-in-production-random-hash"):
            import logging
            logging.getLogger("app.core.config").warning(
                "ВНИМАНИЕ БЕЗОПАСНОСТИ: Используется стандартный секретный ключ JWT. "
                "Обязательно замените settings.auth.jwt.secret_key в продакшн-окружении или через переменную JWT_SECRET_KEY!"
            )
        return inst.validate_production_security()


settings = Settings.load_from_yaml()
