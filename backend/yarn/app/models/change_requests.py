from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

from app.models.yarn import DraftQueueItem, DiffItem


class ChangeRequestCreate(BaseModel):
    cluster_id: str
    title: str = Field(..., min_length=3, max_length=150)
    description: Optional[str] = Field(default="", max_length=1000)
    changes: List[DraftQueueItem] = Field(..., min_length=1)


class ChangeRequestReview(BaseModel):
    comment: Optional[str] = Field(default="", max_length=1000)


class ChangeRequestSummary(BaseModel):
    id: int
    cluster_id: str
    title: str
    status: str  # SUBMITTED, APPROVED, REJECTED, CANCELLED
    author: str
    changes_count: int
    created_at: str
    updated_at: str
    reviewer: Optional[str] = None
    reviewed_at: Optional[str] = None
    deployment_status: Optional[str] = None  # PENDING, DEPLOYING, SUCCESS, FAILED, ROLLED_BACK
    awx_job_id: Optional[int] = None
    deployed_at: Optional[str] = None


class ChangeRequestResponse(BaseModel):
    id: int
    cluster_id: str
    title: str
    description: str
    status: str
    author: str
    created_at: str
    updated_at: str
    reviewer: Optional[str] = None
    review_comment: Optional[str] = None
    reviewed_at: Optional[str] = None
    changes: List[DraftQueueItem]
    diffs: List[DiffItem]
    xml_content: Optional[str] = None
    deployment_status: Optional[str] = None  # PENDING, DEPLOYING, SUCCESS, FAILED, ROLLED_BACK
    awx_job_id: Optional[int] = None
    deployed_at: Optional[str] = None
    deployment_error: Optional[str] = None


class DeployResponse(BaseModel):
    cr_id: int
    cluster_id: str
    awx_job_id: int
    status: str
    message: str
    deployed_at: Optional[str] = None
    stdout: Optional[str] = None
