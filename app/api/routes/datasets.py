from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlmodel import Session

from app.db.session import get_session
from app.schemas.dataset import DatasetSummary, DatasetUploadResponse
from app.services import dataset_service


datasets_router = APIRouter(prefix="/datasets", tags=["datasets"])


@datasets_router.post("", response_model=DatasetUploadResponse)
async def upload_dataset(
    *,
    session: Session = Depends(get_session),
    file: UploadFile = File(...),
    name: str = Form(...),
    description: str | None = Form(default=None),
) -> DatasetUploadResponse:
    dataset = dataset_service.create_dataset(
        session=session,
        upload=file,
        name=name,
        description=description,
    )
    return DatasetUploadResponse(
        id=dataset.id,
        name=dataset.name,
        description=dataset.description,
        original_filename=dataset.original_filename,
        stored_path=dataset.stored_path,
        uploaded_at=dataset.uploaded_at,
        row_count=dataset.row_count,
        column_names=dataset.column_names,
    )


@datasets_router.get("", response_model=list[DatasetSummary])
def list_datasets(session: Session = Depends(get_session)) -> list[DatasetSummary]:
    datasets = dataset_service.list_datasets(session)
    return [
        DatasetSummary(
            id=d.id,
            name=d.name,
            description=d.description,
            uploaded_at=d.uploaded_at,
            row_count=d.row_count,
        )
        for d in datasets
    ]


@datasets_router.get("/{dataset_id}", response_model=DatasetUploadResponse)
def get_dataset(dataset_id: int, session: Session = Depends(get_session)) -> DatasetUploadResponse:
    dataset = dataset_service.get_dataset(session, dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return DatasetUploadResponse(
        id=dataset.id,
        name=dataset.name,
        description=dataset.description,
        original_filename=dataset.original_filename,
        stored_path=dataset.stored_path,
        uploaded_at=dataset.uploaded_at,
        row_count=dataset.row_count,
        column_names=dataset.column_names,
    )

