"""Pydantic models and the job state machine."""
from enum import Enum
from pydantic import BaseModel, Field
from typing import Any, Optional


class JobStatus(str, Enum):
    QUEUED = "queued"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class SubmitJobRequest(BaseModel):
    image_url: str = Field(..., description="Source image to thumbnail")
    width: int = Field(default=128, ge=1, le=4096)
    height: int = Field(default=128, ge=1, le=4096)


class SubmitJobResponse(BaseModel):
    job_id: str
    status: JobStatus
    status_url: str


class JobRecord(BaseModel):
    job_id: str
    status: JobStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: float
    updated_at: float