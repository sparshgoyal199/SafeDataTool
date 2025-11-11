from typing import Optional
from datetime import datetime, timedelta

from sqlalchemy import Column, JSON
from sqlmodel import Field, Relationship, SQLModel


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True, max_length=50)  # Add max_length for DB constraint
    email: str = Field(index=True, max_length=255)
    password: str  # Rename to hashed; hash in service
    # No confirm_password here—validate in schema


class OTPVerification(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True)
    otp: str = Field(index=True, unique=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime = Field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=5))
    pendinguser: list["PendingUser"] = Relationship(back_populates="otp", passive_deletes="all")


class PendingUser(SQLModel, table=True):
    id: int = Field(foreign_key="otpverification.id", ondelete="CASCADE", primary_key=True)
    username: str
    email: str
    password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime = Field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=5))
    otp: OTPVerification = Relationship(back_populates="pendinguser")


class Dataset(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: Optional[str] = Field(default=None)
    original_filename: str
    stored_path: str
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    row_count: Optional[int] = None
    column_names: Optional[list[str]] = Field(sa_column=Column(JSON, nullable=True))

    pipeline_runs: list["PipelineRun"] = Relationship(back_populates="dataset")


class PipelineConfig(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: Optional[str] = None
    quasi_identifiers: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    sensitive_attributes: Optional[list[str]] = Field(sa_column=Column(JSON, nullable=True))
    privacy_technique: str = Field(default="k_anonymity")
    privacy_parameters: dict = Field(default_factory=dict, sa_column=Column(JSON, nullable=True))
    created_at: datetime = Field(default_factory=datetime.utcnow)

    pipeline_runs: list["PipelineRun"] = Relationship(back_populates="config")


class PipelineRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int = Field(foreign_key="dataset.id")
    config_id: int = Field(foreign_key="pipelineconfig.id")
    identifier_path: Optional[str] = None
    protected_path: Optional[str] = None
    status: str = Field(default="created")
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    risk_summary: Optional[dict] = Field(sa_column=Column(JSON, nullable=True))
    utility_summary: Optional[dict] = Field(sa_column=Column(JSON, nullable=True))
    privacy_summary: Optional[dict] = Field(sa_column=Column(JSON, nullable=True))
    report_html_path: Optional[str] = None
    report_pdf_path: Optional[str] = None

    dataset: Dataset = Relationship(back_populates="pipeline_runs")
    config: PipelineConfig = Relationship(back_populates="pipeline_runs")
    risk_metrics: list["RiskMetric"] = Relationship(back_populates="run")
    utility_metrics: list["UtilityMetric"] = Relationship(back_populates="run")


class RiskMetric(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(foreign_key="pipelinerun.id", ondelete="CASCADE")
    name: str
    value: float
    label: str
    details: Optional[dict] = Field(sa_column=Column(JSON, nullable=True))

    run: PipelineRun = Relationship(back_populates="risk_metrics")


class UtilityMetric(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(foreign_key="pipelinerun.id", ondelete="CASCADE")
    name: str
    value: float
    label: str
    details: Optional[dict] = Field(sa_column=Column(JSON, nullable=True))

    run: PipelineRun = Relationship(back_populates="utility_metrics")


class Report(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(foreign_key="pipelinerun.id", ondelete="CASCADE")
    html_path: str
    pdf_path: Optional[str] = None
    generated_at: datetime = Field(default_factory=datetime.utcnow)

    run: PipelineRun = Relationship()
