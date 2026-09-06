from typing import List, Optional, Dict
from pydantic import BaseModel, Field
from app.models.auth import Role


class ClusterResources(BaseModel):
    memory_mb: int = Field(..., description="Общая память кластера в мегабайтах")
    vcores: int = Field(..., description="Общее число vcores в кластере")


class RoleMapping(BaseModel):
    users: List[str] = Field(default_factory=list)
    groups: List[str] = Field(default_factory=list)


class RolesConfig(BaseModel):
    admin: RoleMapping = Field(default_factory=RoleMapping)
    writer: RoleMapping = Field(default_factory=RoleMapping)
    reader: RoleMapping = Field(default_factory=RoleMapping)


class ClusterAcl(BaseModel):
    allowed_users: List[str] = Field(default_factory=lambda: ["*"])
    allowed_groups: List[str] = Field(default_factory=lambda: ["*"])
    roles: RolesConfig = Field(default_factory=RolesConfig)


class ClusterConfig(BaseModel):
    id: str
    name: str
    description: Optional[str] = ""
    resource_manager_urls: List[str] = Field(default_factory=list)
    kerberos_enabled: bool = False
    kerberos_principal: Optional[str] = None
    impersonation_enabled: bool = True
    default_partition: str = "DEFAULT"
    partitions: List[str] = Field(default_factory=lambda: ["DEFAULT"])
    resource_mode: str = "percentage"  # percentage | absolute
    queue_mappings: Optional[str] = "u:%user:%user,g:hadoop-admins:root.production"
    queue_mappings_override: bool = False
    capacity_scheduler_xml_path: Optional[str] = None
    total_resources: ClusterResources
    acl: ClusterAcl = Field(default_factory=ClusterAcl)


class ClusterSummary(BaseModel):
    id: str
    name: str
    description: Optional[str] = ""
    active_rm_url: Optional[str] = None
    kerberos_enabled: bool
    impersonation_enabled: bool
    partitions: List[str]
    default_partition: str
    resource_mode: str
    total_resources: ClusterResources
    user_role: Role  # Роль текущего пользователя в данном кластере
    can_write: bool
    can_admin: bool
