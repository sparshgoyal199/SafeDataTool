from __future__ import annotations

import secrets
import shutil
from pathlib import Path
from typing import Iterable

import pandas as pd
from fastapi import UploadFile
from sqlmodel import Session, select

from app.config import get_settings
from app.db.models import Dataset


def _safe_filename(original_name: str) -> str:
    token = secrets.token_hex(8)
    sanitized = original_name.replace(" ", "_")
    return f"{token}_{sanitized}"


def _load_dataframe(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file format: {path.suffix}")


def create_dataset(
    *,
    session: Session,
    upload: UploadFile,
    name: str,
    description: str | None = None,
) -> Dataset:
    settings = get_settings()
    raw_dir = settings.raw_data_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    stored_filename = _safe_filename(upload.filename or "dataset.csv")
    stored_path = raw_dir / stored_filename

    with stored_path.open("wb") as buffer:
        shutil.copyfileobj(upload.file, buffer)

    df = _load_dataframe(stored_path)

    dataset = Dataset(
        name=name,
        description=description,
        original_filename=upload.filename or stored_filename,
        stored_path=str(stored_path),
        row_count=len(df),
        column_names=list(map(str, df.columns)),
    )
    session.add(dataset)
    session.commit()
    session.refresh(dataset)
    return dataset


def list_datasets(session: Session) -> list[Dataset]:
    result = session.exec(select(Dataset).order_by(Dataset.uploaded_at.desc()))
    return list(result.all())


def get_dataset(session: Session, dataset_id: int) -> Dataset | None:
    return session.get(Dataset, dataset_id)


def resolve_dataset_path(dataset: Dataset) -> Path:
    path = Path(dataset.stored_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset path does not exist: {path}")
    return path

