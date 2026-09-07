import os
from pathlib import Path
from typing import List, Optional, Dict, Any
import yaml
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings

from app.models.cluster import ClusterConfig


class ServerSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    clusters_config_path: str = "config/clusters.yaml"
    cors_origins: List[str] = Field(default_factory=lambda: ["http://localhost:3000", "http://127.0.0.1:3000"])


class DatabaseSettings(BaseModel):
    url: str = "sqlite:////tmp/hdfs_tokens.db"


class SecuritySettings(BaseModel):
    secret_key: str = "change-this-in-production-secret-key-32-chars-long"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 480
    cookie_name: str = "hdfs_explorer_session"
    cookie_secure: bool = False
    cookie_samesite: str = "lax"


from backend.common.core.ldap_auth import CommonLdapConfig, LdapConfig

# LdapSettings как канонический CommonLdapConfig
LdapSettings = CommonLdapConfig


class KerberosSsoSettings(BaseModel):
    enabled: bool = False
    service_principal: str = "HTTP/localhost@EXAMPLE.COM"
    keytab_path: str = ""


class GlobalAclSettings(BaseModel):
    allow_all_authenticated: bool = False
    allowed_groups: List[str] = Field(default_factory=list)
    allowed_users: List[str] = Field(default_factory=list)
    admin_groups: List[str] = Field(default_factory=list)


class MockUser(BaseModel):
    username: str
    password: str
    display_name: str
    email: Optional[str] = None
    groups: List[str] = Field(default_factory=list)


class AuthSettings(BaseModel):
    mode: str = "mock"  # mock | ldaps_only | kerberos_only | hybrid


class AppSettings(BaseSettings):
    server: ServerSettings = Field(default_factory=ServerSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    ldap: LdapSettings = Field(default_factory=LdapSettings)
    mock_users: List[MockUser] = Field(default_factory=list)
    kerberos_sso: KerberosSsoSettings = Field(default_factory=KerberosSsoSettings)
    acl: GlobalAclSettings = Field(default_factory=GlobalAclSettings)
    clusters: List[ClusterConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_production_security(self) -> "AppSettings":
        if not self.server.debug:
            self.security.cookie_secure = True
            default_keys = (
                "change-this-in-production-secret-key-32-chars-long",
                "dev-secret-key-for-local-testing-replace-in-prod"
            )
            if self.security.secret_key in default_keys or len(self.security.secret_key) < 32:
                import logging
                logging.getLogger("security").critical(
                    "ВНИМАНИЕ: Используется стандартный или ненадежный secret_key! "
                    "Необходимо задать уникальный HDFS_SECRET_KEY в переменных окружения."
                )

            # Защита от использования mock-пользователей в production
            if self.auth.mode == "mock" or not self.ldap.enabled:
                raise ValueError(
                    "Mock authentication cannot be used in production mode (debug=False). "
                    "Please enable LDAP authentication or set debug=True for local development."
                )
        return self

    @classmethod
    def load_from_yaml(cls, path: Optional[str] = None) -> "AppSettings":
        config_data: Dict[str, Any] = {}
        if path:
            file_path = Path(path)
        else:
            env_cfg = os.getenv("CONFIG_PATH") or os.getenv("HDFS_CONFIG_PATH")
            if env_cfg:
                file_path = Path(env_cfg)
            else:
                project_cfg = Path(__file__).resolve().parents[3] / "config" / "config.yaml"
                if project_cfg.exists():
                    file_path = project_cfg
                else:
                    file_path = Path("config/config.yaml")

        if not file_path.exists():
            alt_path = Path(__file__).resolve().parents[3] / "config" / "config.yaml"
            if alt_path.exists():
                file_path = alt_path

        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
                if isinstance(loaded, dict):
                    config_data = loaded

        # Переопределения из окружения
        settings = cls(**config_data)
        if os.getenv("STORAGE_URL") or os.getenv("REDIS_URL"):
            settings.database.url = os.getenv("STORAGE_URL") or os.getenv("REDIS_URL")
        if os.getenv("HDFS_DATABASE_URL") or os.getenv("DATABASE_URL"):
            settings.database.url = os.getenv("HDFS_DATABASE_URL") or os.getenv("DATABASE_URL")

        if os.getenv("HDFS_SECRET_KEY") or os.getenv("JWT_SECRET_KEY"):
            settings.security.secret_key = os.getenv("HDFS_SECRET_KEY") or os.getenv("JWT_SECRET_KEY")
        if os.getenv("HDFS_LDAP_PASSWORD") or os.getenv("LDAP_BIND_PASSWORD"):
            settings.ldap.bind_password = os.getenv("HDFS_LDAP_PASSWORD") or os.getenv("LDAP_BIND_PASSWORD")
        if os.getenv("HDFS_LDAP_URI") or os.getenv("LDAP_SERVER_URI"):
            settings.ldap.server_uri = os.getenv("HDFS_LDAP_URI") or os.getenv("LDAP_SERVER_URI")
        if os.getenv("HDFS_CLUSTERS_PATH"):
            settings.server.clusters_config_path = os.getenv("HDFS_CLUSTERS_PATH")

        return settings


class ClusterRegistry:
    def __init__(self, config_path: str = "config/clusters.yaml"):
        self.config_path = config_path
        self.clusters: Dict[str, ClusterConfig] = {}
        self.reload()

    def reload(self):
        file_path = Path(self.config_path)
        if not file_path.exists():
            alt_path = Path(__file__).resolve().parent.parent.parent.parent / self.config_path
            if alt_path.exists():
                file_path = alt_path

        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
                raw_clusters = data.get("clusters", [])
                self.clusters = {
                    c["id"]: ClusterConfig(**c) for c in raw_clusters
                }
        elif settings.clusters:
            self.clusters = {c.id: c for c in settings.clusters}
        else:
            self.clusters = {}

    def get(self, cluster_id: str) -> Optional[ClusterConfig]:
        return self.clusters.get(cluster_id)

    def all(self) -> List[ClusterConfig]:
        return list(self.clusters.values())


# Глобальные инстансы настроек
_cfg_path = os.getenv("CONFIG_PATH") or os.getenv("HDFS_CONFIG_PATH")
settings = AppSettings.load_from_yaml(_cfg_path)
cluster_registry = ClusterRegistry(settings.server.clusters_config_path)
if not cluster_registry.clusters and settings.clusters:
    cluster_registry.clusters = {c.id: c for c in settings.clusters}
