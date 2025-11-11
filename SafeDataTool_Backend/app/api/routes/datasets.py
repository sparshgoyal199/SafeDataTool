from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlmodel import Session

from app.core.security import get_current_user
from app.db.session import get_session
from app.schemas.dataset import DatasetSummary, DatasetUploadResponse
from app.services.dataset_service import get_dataset, list_datasets, save_uploaded_dataset

datasets_router = APIRouter(prefix="/datasets", tags=["datasets"])


@datasets_router.post("", response_model=DatasetUploadResponse)
async def upload_dataset(
    *,
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
    file: UploadFile = File(...),
    name: str = Form(...),
    description: str | None = Form(default=None),
) -> DatasetUploadResponse:
    """Upload a new dataset."""
    try:
        dataset = save_uploaded_dataset(
            upload=file,
            name=name,
            description=description,
            session=session,
        )
        return DatasetUploadResponse.model_validate(dataset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload dataset: {str(e)}")


@datasets_router.get("", response_model=list[DatasetSummary])
def list_datasets_route(
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
) -> list[DatasetSummary]:
    """List all uploaded datasets."""
    datasets = list_datasets(session)
    return [DatasetSummary.model_validate(dataset) for dataset in datasets]


@datasets_router.get("/{dataset_id}", response_model=DatasetUploadResponse)
def get_dataset_route(
    dataset_id: int,
    session: Session = Depends(get_session),
    current_user: int = Depends(get_current_user),
) -> DatasetUploadResponse:
    """Get dataset details by ID."""
    dataset = get_dataset(dataset_id, session)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return DatasetUploadResponse.model_validate(dataset)
