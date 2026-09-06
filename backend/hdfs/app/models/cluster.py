from typing import List, Optional
from pydantic import BaseModel, Field


class ClusterAcl(BaseModel):
    allowed_groups: List[str] = Field(default_factory=list)
    allowed_users: List[str] = Field(default_factory=list)
    read_only_groups: List[str] = Field(default_factory=list)
    admin_groups: List[str] = Field(default_factory=list)


class ClusterConfig(BaseModel):
    id: str
    name: str
    description: str = ""
    webhdfs_urls: List[str]
    auth_type: str = "simple"  # "simple" or "kerberos"
    service_principal: Optional[str] = None
    keytab_path: Optional[str] = None
    timeout_seconds: int = 30
    preview_max_bytes: int = 1048576  # 1 MB
    default_path: str = "/user/{username}"
    mock_storage: bool = False
    acl: ClusterAcl = Field(default_factory=ClusterAcl)


class ClusterPublicInfo(BaseModel):
    id: str
    name: str
    description: str
    default_path: str
    is_read_only: bool
    is_admin: bool
