from datetime import datetime
from pathlib import Path

from sqlmodel import Session, select

from app.db.models import Dataset, PipelineConfig, PipelineRun, RiskMetric, UtilityMetric
from app.db.session import engine
from app.pipeline.orchestrator import PipelineOrchestrator
from app.workers.celery_app import celery_app


@celery_app.task(bind=True, name="execute_pipeline")
def execute_pipeline_task(self, run_id: int):
    """
    Background task to execute the SafeData pipeline.

    Args:
        run_id: ID of the PipelineRun to execute

    Returns:
        Dictionary with execution status and results
    """
    with Session(engine) as session:
        run = session.get(PipelineRun, run_id)
        if not run:
            return {"status": "error", "message": f"Run {run_id} not found"}

        try:
            # Update status
            run.status = "running"
            session.add(run)
            session.commit()

            # Get dataset and config
            dataset = session.get(Dataset, run.dataset_id)
            config = session.get(PipelineConfig, run.config_id)

            if not dataset or not config:
                raise ValueError("Dataset or config not found")

            # Execute pipeline
            orchestrator = PipelineOrchestrator()
            dataset_path = Path(dataset.stored_path)
            identifier_path = Path(run.identifier_path) if run.identifier_path else None

            artifacts = orchestrator.run(
                dataset_path=dataset_path,
                config=config,
                identifier_path=identifier_path,
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

            return {
                "status": "completed",
                "run_id": run_id,
                "protected_path": run.protected_path,
                "report_path": run.report_html_path,
            }

        except Exception as exc:
            run.status = "failed"
            run.completed_at = datetime.utcnow()
            session.add(run)
            session.commit()
            return {"status": "failed", "run_id": run_id, "error": str(exc)}

