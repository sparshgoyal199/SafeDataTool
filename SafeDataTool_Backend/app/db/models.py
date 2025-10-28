from typing import Optional
from sqlmodel import SQLModel, Field

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True, max_length=50)  # Add max_length for DB constraint
    email: str = Field(index=True, unique=True, max_length=255)
    password: str  # Rename to hashed; hash in service
    # No confirm_password here—validate in schema