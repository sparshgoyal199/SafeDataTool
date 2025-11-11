from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

from sqlmodel import Session, select

from app.config import get_settings
from app.db.models import (
    Dataset,
    PipelineConfig,
    PipelineRun,
    Report,
    RiskMetric,
    UtilityMetric,
)
from app.pipeline import PipelineOrchestrator
from app.pipeline.risk import RiskMetricResult
from app.pipeline.utility import UtilityMetricResult
from app.schemas.pipeline import PipelineConfigCreate, PipelineConfigUpdate
from app.services.dataset_service import resolve_dataset_path


def list_configs(session: Session) -> list[PipelineConfig]:
    result = session.exec(select(PipelineConfig).order_by(PipelineConfig.created_at.desc()))
    return list(result.all())


def create_config(session: Session, config_in: PipelineConfigCreate) -> PipelineConfig:
    config = PipelineConfig.model_validate(config_in)
    session.add(config)
    session.commit()
    session.refresh(config)
    return config


def update_config(
    session: Session,
    config: PipelineConfig,
    update_in: PipelineConfigUpdate,
) -> PipelineConfig:
    update_data = update_in.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(config, key, value)
    session.add(config)
    session.commit()
    session.refresh(config)
    return config


def get_config(session: Session, config_id: int) -> PipelineConfig | None:
    return session.get(PipelineConfig, config_id)


def create_run(
    session: Session,
    *,
    dataset: Dataset,
    config: PipelineConfig,
    identifier_dataset: Dataset | None = None,
) -> PipelineRun:
    run = PipelineRun(
        dataset_id=dataset.id,
        config_id=config.id,
        identifier_path=str(resolve_dataset_path(identifier_dataset)) if identifier_dataset else None,
        status="created",
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def execute_run(session: Session, run: PipelineRun) -> PipelineRun:
    settings = get_settings()
    orchestrator = PipelineOrchestrator(settings=settings)

    run.status = "running"
    run.started_at = datetime.utcnow()
    session.add(run)
    session.commit()
    session.refresh(run)

    dataset = run.dataset
    config = run.config
    dataset_path = resolve_dataset_path(dataset)
    identifier_path = Path(run.identifier_path) if run.identifier_path else None

    artifacts = orchestrator.run(
        dataset_path=dataset_path,
        config=config,
        identifier_path=identifier_path,
        run_id=str(run.id),
    )

    run.status = "completed"
    run.completed_at = datetime.utcnow()
    run.protected_path = str(artifacts.protected_dataset_path)
    run.report_html_path = str(artifacts.report_html_path)
    run.report_pdf_path = str(artifacts.report_pdf_path) if artifacts.report_pdf_path else None
    run.risk_summary = _metrics_to_summary(artifacts.risk_metrics)
    run.utility_summary = _metrics_to_summary(artifacts.utility_metrics)
    run.privacy_summary = artifacts.privacy_info

    _persist_metrics(session, run, artifacts.risk_metrics, artifacts.utility_metrics)
    _ensure_report_record(session, run)

    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def list_runs(session: Session) -> list[PipelineRun]:
    result = session.exec(select(PipelineRun).order_by(PipelineRun.started_at.desc()))
    return list(result.all())


def _metrics_to_summary(metrics: Iterable[RiskMetricResult | UtilityMetricResult]) -> dict[str, float]:
    summary: dict[str, float] = {}
    for metric in metrics:
        summary[metric.name] = float(metric.value)
    return summary


def _persist_metrics(
    session: Session,
    run: PipelineRun,
    risk_metrics: Iterable[RiskMetricResult],
    utility_metrics: Iterable[UtilityMetricResult],
) -> None:
    for metric in risk_metrics:
        session.add(
            RiskMetric(
                run_id=run.id,
                name=metric.name,
                value=float(metric.value),
                label=metric.label,
                details=metric.details,
            )
        )

    for metric in utility_metrics:
        session.add(
            UtilityMetric(
                run_id=run.id,
                name=metric.name,
                value=float(metric.value),
                label=metric.label,
                details=metric.details,
            )
        )


def _ensure_report_record(session: Session, run: PipelineRun) -> None:
    if not run.report_html_path:
        return
    report = Report(
        run_id=run.id,
        html_path=run.report_html_path,
        pdf_path=run.report_pdf_path,
    )
    session.add(report)

