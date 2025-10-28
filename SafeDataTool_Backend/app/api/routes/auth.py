from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session
from app.db.session import get_session
from app.schemas.user import UserCreate, UserOut
from app.services.user_service import create_user

auth_router = APIRouter(prefix="/auth", tags=["auth"])

#have created similar routes for all the distinct features
#it means created names for all these routes for grouping similar routes - that's why we use APIRouter
@auth_router.post("/signup", response_model=UserOut)
def signup(user: UserCreate, session: Session = Depends(get_session)):
    try:
        return create_user(user, session)
    except ValueError as e:
        raise HTTPException(400, str(e))
    