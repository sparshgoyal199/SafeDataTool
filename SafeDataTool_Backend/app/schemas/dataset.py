from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class DatasetUploadResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    original_filename: str
    row_count: Optional[int]
    column_names: Optional[list[str]]
    uploaded_at: datetime

    model_config = ConfigDict(from_attributes=True)


class DatasetSummary(BaseModel):
    id: int
    name: str
    description: Optional[str]
    row_count: Optional[int]
    uploaded_at: datetime

    model_config = ConfigDict(from_attributes=True)

