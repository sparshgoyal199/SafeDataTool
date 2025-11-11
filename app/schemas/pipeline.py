from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field
from sqlmodel import SQLModel


class PipelineConfigBase(SQLModel):
    name: str
    description: Optional[str] = None
    quasi_identifiers: list[str] = Field(default_factory=list)
    sensitive_attributes: Optional[list[str]] = None
    privacy_technique: str = Field(default="k_anonymity")
    privacy_parameters: dict[str, Any] = Field(default_factory=dict)


class PipelineConfigCreate(PipelineConfigBase):
    pass


class PipelineConfigUpdate(SQLModel):
    name: Optional[str] = None
    description: Optional[str] = None
    quasi_identifiers: Optional[list[str]] = None
    sensitive_attributes: Optional[list[str]] = None
    privacy_technique: Optional[str] = None
    privacy_parameters: Optional[dict[str, Any]] = None


class PipelineConfigOut(PipelineConfigBase):
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PipelineRunCreate(BaseModel):
    dataset_id: int
    config_id: int
    identifier_dataset_id: Optional[int] = Field(
        default=None,
        description="Optional dataset identifier referencing a ground-truth file for linkage testing.",
    )


class PipelineRunStatus(BaseModel):
    id: int
    status: str
    started_at: datetime
    completed_at: Optional[datetime]

    model_config = ConfigDict(from_attributes=True)


class PipelineRunDetail(PipelineRunStatus):
    dataset_id: int
    config_id: int
    protected_path: Optional[Path]
    report_html_path: Optional[Path]
    report_pdf_path: Optional[Path]
    risk_summary: Optional[dict[str, Any]]
    utility_summary: Optional[dict[str, Any]]
    privacy_summary: Optional[dict[str, Any]]

