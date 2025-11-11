from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlmodel import Session, select

from app.config import get_settings
from app.db.models import Dataset, PipelineConfig, PipelineRun, RiskMetric, UtilityMetric
from app.pipeline.orchestrator import PipelineOrchestrator
from app.schemas.pipeline import PipelineConfigCreate, PipelineConfigOut, PipelineConfigUpdate, PipelineRunDetail

settings = get_settings()


def create_pipeline_config(config_in: PipelineConfigCreate, session: Session) -> PipelineConfig:
    config = PipelineConfig(
        name=config_in.name,
        description=config_in.description,
        quasi_identifiers=config_in.quasi_identifiers,
        sensitive_attributes=config_in.sensitive_attributes,
        privacy_technique=config_in.privacy_technique,
        privacy_parameters=config_in.privacy_parameters,
    )
    session.add(config)
    session.commit()
    session.refresh(config)
    return config


def get_pipeline_config(config_id: int, session: Session) -> Optional[PipelineConfig]:
    return session.get(PipelineConfig, config_id)


def list_pipeline_configs(session: Session) -> list[PipelineConfig]:
    return list(session.exec(select(PipelineConfig)).all())


def update_pipeline_config(config_id: int, update_in: PipelineConfigUpdate, session: Session) -> Optional[PipelineConfig]:
    config = session.get(PipelineConfig, config_id)
    if not config:
        return None

    if update_in.name is not None:
        config.name = update_in.name
    if update_in.description is not None:
        config.description = update_in.description
    if update_in.quasi_identifiers is not None:
        config.quasi_identifiers = update_in.quasi_identifiers
    if update_in.sensitive_attributes is not None:
        config.sensitive_attributes = update_in.sensitive_attributes
    if update_in.privacy_technique is not None:
        config.privacy_technique = update_in.privacy_technique
    if update_in.privacy_parameters is not None:
        config.privacy_parameters = update_in.privacy_parameters

    session.add(config)
    session.commit()
    session.refresh(config)
    return config


def create_pipeline_run(dataset_id: int, config_id: int, identifier_path: Optional[str], session: Session) -> PipelineRun:
    dataset = session.get(Dataset, dataset_id)
    config = session.get(PipelineConfig, config_id)

    if not dataset:
        raise ValueError(f"Dataset {dataset_id} not found")
    if not config:
        raise ValueError(f"Pipeline config {config_id} not found")

    run = PipelineRun(
        dataset_id=dataset_id,
        config_id=config_id,
        identifier_path=identifier_path,
        status="created",
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    # Execute pipeline (synchronously or asynchronously based on config)
    if settings.use_background_jobs:
        # Queue background job
        from app.workers.pipeline_tasks import execute_pipeline_task

        task = execute_pipeline_task.delay(run.id)
        run.status = "queued"
        session.add(run)
        session.commit()
        session.refresh(run)
    else:
        # Execute synchronously
        try:
            run.status = "running"
            session.add(run)
            session.commit()

            orchestrator = PipelineOrchestrator()
            dataset_path = Path(dataset.stored_path)
            artifacts = orchestrator.run(
                dataset_path=dataset_path,
                config=config,
                identifier_path=Path(identifier_path) if identifier_path else None,
                run_id=str(run.id),
            )

            # Update run with results
            run.status = "completed"
            run.completed_at = datetime.utcnow()
            run.protected_path = str(artifacts.get("protected_path", ""))
            run.report_html_path = str(artifacts.get("report_html_path", ""))
            run.report_pdf_path = str(artifacts.get("report_pdf_path", ""))
            run.risk_summary = artifacts.get("risk_summary", {})
            run.utility_summary = artifacts.get("utility_summary", {})
            run.privacy_summary = artifacts.get("privacy_summary", {})

            # Store metrics
            for metric_data in artifacts.get("risk_metrics", []):
                metric = RiskMetric(
                    run_id=run.id,
                    name=metric_data["name"],
                    value=metric_data["value"],
                    label=metric_data["label"],
                    details=metric_data.get("details"),
                )
                session.add(metric)

            for metric_data in artifacts.get("utility_metrics", []):
                metric = UtilityMetric(
                    run_id=run.id,
                    name=metric_data["name"],
                    value=metric_data["value"],
                    label=metric_data["label"],
                    details=metric_data.get("details"),
                )
                session.add(metric)

            session.add(run)
            session.commit()
            session.refresh(run)

        except Exception as exc:
            run.status = "failed"
            run.completed_at = datetime.utcnow()
            session.add(run)
            session.commit()
            session.refresh(run)
            raise exc

    return run


def list_pipeline_runs(session: Session) -> list[PipelineRun]:
    return list(session.exec(select(PipelineRun).order_by(PipelineRun.started_at.desc())).all())


def get_pipeline_run(run_id: int, session: Session) -> Optional[PipelineRun]:
    run = session.get(PipelineRun, run_id)
    if run:
        # Eager load relationships
        _ = run.dataset
        _ = run.config
        _ = list(run.risk_metrics)
        _ = list(run.utility_metrics)
    return run


def _to_run_detail(run: PipelineRun) -> PipelineRunDetail:
    return PipelineRunDetail.model_validate(
        {
            "id": run.id,
            "status": run.status,
            "started_at": run.started_at,
            "completed_at": run.completed_at,
            "dataset_id": run.dataset_id,
            "config_id": run.config_id,
            "protected_path": Path(run.protected_path) if run.protected_path else None,
            "report_html_path": Path(run.report_html_path) if run.report_html_path else None,
            "report_pdf_path": Path(run.report_pdf_path) if run.report_pdf_path else None,
            "risk_summary": run.risk_summary,
            "utility_summary": run.utility_summary,
            "privacy_summary": run.privacy_summary,
            "risk_metrics": [
                {
                    "name": metric.name,
                    "value": float(metric.value),
                    "label": metric.label,
                    "details": metric.details,
                }
                for metric in run.risk_metrics
            ],
            "utility_metrics": [
                {
                    "name": metric.name,
                    "value": float(metric.value),
                    "label": metric.label,
                    "details": metric.details,
                }
                for metric in run.utility_metrics
            ],
        }
    )

