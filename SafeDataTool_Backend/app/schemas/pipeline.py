from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class PipelineConfigCreate(BaseModel):
    name: str
    description: Optional[str] = None
    quasi_identifiers: list[str] = Field(default_factory=list)
    sensitive_attributes: Optional[list[str]] = None
    privacy_technique: str = Field(default="k_anonymity")
    privacy_parameters: dict[str, Any] = Field(default_factory=dict)


class PipelineConfigUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    quasi_identifiers: Optional[list[str]] = None
    sensitive_attributes: Optional[list[str]] = None
    privacy_technique: Optional[str] = None
    privacy_parameters: Optional[dict[str, Any]] = None


class PipelineConfigOut(BaseModel):
    id: int
    name: str
    description: Optional[str]
    quasi_identifiers: list[str]
    sensitive_attributes: Optional[list[str]]
    privacy_technique: str
    privacy_parameters: dict[str, Any]
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PipelineRunCreate(BaseModel):
    dataset_id: int
    config_id: int
    identifier_path: Optional[str] = None


class PipelineRunStatus(BaseModel):
    id: int
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class MetricOut(BaseModel):
    name: str
    value: float
    label: str
    details: Optional[dict[str, Any]] = None


class PipelineRunDetail(PipelineRunStatus):
    dataset_id: int
    config_id: int
    protected_path: Optional[Path] = None
    report_html_path: Optional[Path] = None
    report_pdf_path: Optional[Path] = None
    risk_summary: Optional[dict[str, Any]] = None
    utility_summary: Optional[dict[str, Any]] = None
    privacy_summary: Optional[dict[str, Any]] = None
    risk_metrics: list[MetricOut] = Field(default_factory=list)
    utility_metrics: list[MetricOut] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)

