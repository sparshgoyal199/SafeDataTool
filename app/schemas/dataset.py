from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field
from sqlmodel import SQLModel


class DatasetBase(SQLModel):
    name: str
    description: Optional[str] = None


class DatasetCreate(DatasetBase):
    pass


class DatasetUploadResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    original_filename: str
    stored_path: Path
    uploaded_at: datetime
    row_count: Optional[int]
    column_names: list[str] | None


class DatasetSummary(BaseModel):
    id: int
    name: str
    description: Optional[str]
    uploaded_at: datetime
    row_count: Optional[int]

