# services/user_service.py
from sqlmodel import Session, select
from app.db.models import User
from app.schemas.user import UserCreate, UserOut
from app.core.security import hash_password

def create_user(user_in: UserCreate,session: Session) -> UserOut:
    # Validate & hash
    from main import DbException
    already_user = session.exec(select(User).where(User.username == user_in.username or User.email == user_in.email)).all()
    print(already_user)
    if already_user:
        raise DbException("username or email already exist")
    db_user = User.model_validate(user_in)
    db_user.password = hash_password(user_in.password)
    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return UserOut.model_validate(db_user)  # Or UserOut.model_validate(db_user)