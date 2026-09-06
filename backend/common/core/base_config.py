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


class BaseLdapSettings(BaseModel):
    enabled: bool = False
    server_uri: str = "ldaps://localhost:636"
    use_ssl: bool = True
    verify_cert: bool = True
    allow_insecure_ssl: bool = False
    ca_cert_file: Optional[str] = None
    bind_dn: str = "cn=admin,dc=example,dc=com"
    bind_password: str = "admin"
    user_search_base: str = "ou=users,dc=example,dc=com"
    user_search_filter: str = "(&(objectClass=inetOrgPerson)(uid={username}))"
    username_attribute: str = "uid"
    email_attribute: str = "mail"
    display_name_attribute: str = "cn"
    group_search_base: str = "ou=groups,dc=example,dc=com"
    group_search_filter: str = "(&(objectClass=groupOfNames)(member={user_dn}))"
    group_attribute: str = "cn"
    use_user_memberof: bool = False
    memberof_attribute: str = "memberOf"


class BaseKerberosSettings(BaseModel):
    enabled: bool = False
    service_principal: Optional[str] = None
    keytab_path: Optional[str] = None
    keytab_file: Optional[str] = None


class BaseDatabaseSettings(BaseModel):
    url: str = "sqlite:////tmp/hadoop_explorer.db"
    redis_url: Optional[str] = None
