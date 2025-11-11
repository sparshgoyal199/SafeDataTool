import pandas as pd
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import UploadFile
from sqlmodel import Session, select

from app.config import get_settings
from app.db.models import Dataset

settings = get_settings()


def save_uploaded_dataset(
    upload: UploadFile,
    name: str,
    description: Optional[str],
    session: Session,
) -> Dataset:
    """
    Save uploaded dataset file and create database record.

    Returns:
        Created Dataset instance
    """
    # Create data directory if it doesn't exist
    data_dir = Path(settings.data_dir) / "raw"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Generate unique filename
    file_ext = Path(upload.filename).suffix
    stored_filename = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{file_ext}"
    stored_path = data_dir / stored_filename

    # Save file
    with stored_path.open("wb") as buffer:
        shutil.copyfileobj(upload.file, buffer)
    upload.file.close()

    # Read dataset to get metadata
    try:
        df = pd.read_csv(stored_path)
        row_count = len(df)
        column_names = list(df.columns)
    except Exception:
        row_count = None
        column_names = None

    # Create database record
    dataset = Dataset(
        name=name,
        description=description,
        original_filename=upload.filename,
        stored_path=str(stored_path),
        row_count=row_count,
        column_names=column_names,
    )

    session.add(dataset)
    session.commit()
    session.refresh(dataset)

    return dataset


def get_dataset(dataset_id: int, session: Session) -> Optional[Dataset]:
    """Get dataset by ID."""
    return session.get(Dataset, dataset_id)


def list_datasets(session: Session) -> list[Dataset]:
    """List all datasets."""
    return list(session.exec(select(Dataset).order_by(Dataset.uploaded_at.desc())).all())

