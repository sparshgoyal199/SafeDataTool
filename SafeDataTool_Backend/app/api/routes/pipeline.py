from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlmodel import Session
from pathlib import Path

from app.core.security import get_current_user
from app.db.models import PipelineConfig, PipelineRun
from app.db.session import get_session
from app.schemas.pipeline import (
    MetricOut,
    PipelineConfigCreate,
    PipelineConfigOut,
    PipelineConfigUpdate,
    PipelineRunCreate,
    PipelineRunDetail,
    PipelineRunStatus,
)
from app.services.pipeline_service import (
    create_pipeline_config,
    create_pipeline_run,
    get_pipeline_config,
    get_pipeline_run,
    list_pipeline_configs,
    list_pipeline_runs,
    update_pipeline_config,
    _to_run_detail,
)

pipeline_router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@pipeline_router.get("/runs/{run_id}/protected")
def download_protected_dataset(
    run_id: int,
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
):
    run = session.get(PipelineRun, run_id)
    if run is None or not run.protected_path:
        raise HTTPException(status_code=404, detail="Protected dataset not found")

    protected_path = Path(run.protected_path)
    if not protected_path.exists():
        raise HTTPException(status_code=404, detail="Protected dataset file is missing on disk")

    media_type = "text/csv" if protected_path.suffix.lower() == ".csv" else "application/octet-stream"
    return FileResponse(path=protected_path, media_type=media_type, filename=protected_path.name)


@pipeline_router.get("/runs/{run_id}/report")
def download_report(
    run_id: int,
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
):
    run = session.get(PipelineRun, run_id)
    if run is None or not run.report_html_path:
        raise HTTPException(status_code=404, detail="Report not found")

    report_path = Path(run.report_html_path)
    if not report_path.exists():
        raise HTTPException(status_code=404, detail="Report file is missing on disk")

    # Prefer PDF if available, otherwise HTML
    if run.report_pdf_path and Path(run.report_pdf_path).exists():
        return FileResponse(path=run.report_pdf_path, media_type="application/pdf", filename=report_path.stem + ".pdf")
    return FileResponse(path=report_path, media_type="text/html", filename=report_path.name)


@pipeline_router.get("/runs/{run_id}", response_model=PipelineRunDetail)
def get_run(
    run_id: int,
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
) -> PipelineRunDetail:
    run = get_pipeline_run(run_id, session)
    if run is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found")
    return _to_run_detail(run)


@pipeline_router.get("/runs", response_model=list[PipelineRunStatus])
def list_runs(
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
) -> list[PipelineRunStatus]:
    runs = list_pipeline_runs(session)
    return [PipelineRunStatus.model_validate(run) for run in runs]


@pipeline_router.get("/configs", response_model=list[PipelineConfigOut])
def list_pipeline_configs_route(
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
) -> list[PipelineConfigOut]:
    configs = list_pipeline_configs(session)
    return [PipelineConfigOut.model_validate(config) for config in configs]


@pipeline_router.post("/configs", response_model=PipelineConfigOut)
def create_pipeline_config_route(
    config_in: PipelineConfigCreate,
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
) -> PipelineConfigOut:
    config = create_pipeline_config(config_in, session)
    return PipelineConfigOut.model_validate(config)


@pipeline_router.put("/configs/{config_id}", response_model=PipelineConfigOut)
def update_pipeline_config_route(
    config_id: int,
    update_in: PipelineConfigUpdate,
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
) -> PipelineConfigOut:
    config = update_pipeline_config(config_id, update_in, session)
    if config is None:
        raise HTTPException(status_code=404, detail="Pipeline config not found")
    return PipelineConfigOut.model_validate(config)


@pipeline_router.post("/runs", response_model=PipelineRunDetail)
def trigger_pipeline_run(
    run_in: PipelineRunCreate,
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
) -> PipelineRunDetail:
    try:
        run = create_pipeline_run(run_in.dataset_id, run_in.config_id, run_in.identifier_path, session)
        return _to_run_detail(run)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {str(e)}")
