from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlmodel import Session

from app.db.session import get_session
from app.schemas.pipeline import (
    PipelineConfigCreate,
    PipelineConfigOut,
    PipelineConfigUpdate,
    PipelineRunCreate,
    PipelineRunDetail,
    PipelineRunStatus,
)
from app.services import dataset_service, pipeline_service


pipeline_router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@pipeline_router.post("/configs", response_model=PipelineConfigOut)
def create_pipeline_config(
    config_in: PipelineConfigCreate,
    session: Session = Depends(get_session),
) -> PipelineConfigOut:
    config = pipeline_service.create_config(session, config_in)
    return PipelineConfigOut.model_validate(config)


@pipeline_router.get("/configs", response_model=list[PipelineConfigOut])
def list_pipeline_configs(session: Session = Depends(get_session)) -> list[PipelineConfigOut]:
    configs = pipeline_service.list_configs(session)
    return [PipelineConfigOut.model_validate(cfg) for cfg in configs]


@pipeline_router.put("/configs/{config_id}", response_model=PipelineConfigOut)
def update_pipeline_config(
    config_id: int,
    update_in: PipelineConfigUpdate,
    session: Session = Depends(get_session),
) -> PipelineConfigOut:
    config = pipeline_service.get_config(session, config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Pipeline configuration not found")

    updated = pipeline_service.update_config(session, config, update_in)
    return PipelineConfigOut.model_validate(updated)


@pipeline_router.post("/runs", response_model=PipelineRunDetail)
def trigger_pipeline_run(
    run_in: PipelineRunCreate,
    session: Session = Depends(get_session),
) -> PipelineRunDetail:
    dataset = dataset_service.get_dataset(session, run_in.dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    config = pipeline_service.get_config(session, run_in.config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Pipeline configuration not found")

    identifier_dataset = None
    if run_in.identifier_dataset_id is not None:
        identifier_dataset = dataset_service.get_dataset(session, run_in.identifier_dataset_id)
        if identifier_dataset is None:
            raise HTTPException(status_code=404, detail="Identifier dataset not found")

    run = pipeline_service.create_run(
        session,
        dataset=dataset,
        config=config,
        identifier_dataset=identifier_dataset,
    )
    completed_run = pipeline_service.execute_run(session, run)
    return _to_run_detail(completed_run)


@pipeline_router.get("/runs", response_model=list[PipelineRunStatus])
def list_runs(session: Session = Depends(get_session)) -> list[PipelineRunStatus]:
    runs = pipeline_service.list_runs(session)
    return [PipelineRunStatus.model_validate(run) for run in runs]


@pipeline_router.get("/runs/{run_id}", response_model=PipelineRunDetail)
def get_run(run_id: int, session: Session = Depends(get_session)) -> PipelineRunDetail:
    run = session.get(pipeline_service.PipelineRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found")
    return _to_run_detail(run)


@pipeline_router.get("/runs/{run_id}/report")
def download_report(run_id: int, session: Session = Depends(get_session)):
    run = session.get(pipeline_service.PipelineRun, run_id)
    if run is None or not run.report_html_path:
        raise HTTPException(status_code=404, detail="Report not found")

    report_path = Path(run.report_pdf_path) if run.report_pdf_path else Path(run.report_html_path)
    if not report_path.exists():
        raise HTTPException(status_code=404, detail="Report file is missing on disk")

    media_type = "application/pdf" if report_path.suffix.lower() == ".pdf" else "text/html"
    return FileResponse(path=report_path, media_type=media_type, filename=report_path.name)


def _to_run_detail(run) -> PipelineRunDetail:
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
        }
    )

