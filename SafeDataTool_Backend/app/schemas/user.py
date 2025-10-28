# schemas/user.py
from typing import Optional
from pydantic import model_validator, EmailStr  # For validation
from sqlmodel import SQLModel
from app.db.models import User  # Reference DB model
# Input schema for signup (validates incoming data)
class UserCreate(SQLModel):
    username: str
    email: str = EmailStr()  # Auto-validates email format (e.g., user@domain.com)
    password: str
    confirm_password: str # Transient field for matching

    @model_validator(mode="after")
    #becuase of @field_validator this class method will run automatically
    #@classmethod
    #classmethod is here because we want to validate the data before creating the object
    #class level funtions are thoese functions which does not run for any specific object-related data
    #class level functions used in validators as the function runs before creating the object(during the time of object creation) and there are other various of class method
    #and this will run automatically not all the class methods runs automatically
    def passwords_match(self) -> str:
        from main import AuthException
        v = self.password
        if self.confirm_password != self.password:
            raise AuthException("Passwords do not match")
    
        if len(v) < 8:
            raise AuthException("Password must be at least 8 characters long")
        if not any(c.isupper() for c in v):
            raise AuthException("Password must contain at least one uppercase letter")
        if not any(c.isdigit() for c in v):
            raise AuthException("Password must contain at least one digit")
        if not any(not c.isalnum() for c in v):  # special character check
            raise AuthException("Password must contain at least one special character")

        if not self.username.isalnum():  # Alphanumeric only
            raise AuthException("Username must be alphanumeric")
        return self

# Response schema (excludes sensitive fields)
class UserOut(SQLModel):
    id: Optional[int]
    username: str
    email: str
    #from_orm
    #Purpose: Create a schema instance from another object (like a DB model or any Python object).
    #Works with: Any object that has attributes with the same names as the schema fields.

    #Key points about from_orm:
    #Reads attributes of an object.
    #Only copies the fields defined in the schema.
    #Ignores extra fields (like password).
    
    class Config:
        from_attributes = True  # Convert DB User obj to dict