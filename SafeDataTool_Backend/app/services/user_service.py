# services/user_service.py
from sqlmodel import Session, select
from app.db.models import User
from app.schemas.user import UserCreate, UserOut, Token
from app.core.security import hash_password, create_access_token

def create_user(user_in: UserCreate,session: Session) -> UserOut:
    # Validate & hash
    already_user = session.exec(select(User).where(User.username == user_in.username or User.email == user_in.email)).all()
    if already_user:
        raise ValueError("username or email already exist")
    db_user = User.model_validate(user_in)
    db_user.password = hash_password(user_in.password)
    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return UserOut.model_validate(db_user)  # Or UserOut.model_validate(db_user)

def login_user(username: str,password: str,session: Session) -> Token:
    # Validate & hash
    hashed_password = hash_password(password)
    db_user = session.exec(select(User).where(User.username == username,User.password == hashed_password)).first()
    if not db_user:
        raise ValueError("please enter correct username or password")
    access_token = create_access_token({"sub": db_user.id})
    return Token(access_token=access_token, token_type="bearer")