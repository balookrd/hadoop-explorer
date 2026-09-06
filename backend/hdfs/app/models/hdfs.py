from typing import List, Optional, Any
from pydantic import BaseModel, Field


class HdfsFileStatus(BaseModel):
    pathSuffix: str
    type: str  # "FILE" or "DIRECTORY"
    length: int
    owner: str
    group: str
    permission: str
    accessTime: int
    modificationTime: int
    blockSize: int = 0
    replication: int = 0
    childrenNum: Optional[int] = 0


class DirectoryListingResponse(BaseModel):
    cluster_id: str
    path: str
    parent_path: Optional[str] = None
    files: List[HdfsFileStatus]
    total_files: int
    total_directories: int
    total_size: int
    can_write: bool = True
    can_read: bool = True


class FilePreviewResponse(BaseModel):
    cluster_id: str
    path: str
    file_type: str  # "text", "json", "csv", "parquet", "binary"
    size: int
    truncated: bool = False
    content: Optional[str] = None
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    row_count: Optional[int] = None
    error: Optional[str] = None


class FileActionResponse(BaseModel):
    success: bool
    message: str
    path: Optional[str] = None


class CrossClusterCopyRequest(BaseModel):
    source_cluster_id: str
    source_path: str
    target_cluster_id: str
    target_path: str
    overwrite: bool = False


class CrossClusterCopyResponse(BaseModel):
    success: bool
    message: str
    source_cluster_id: str
    source_path: str
    target_cluster_id: str
    target_path: str
    copied_files: int = 0
    copied_bytes: int = 0

