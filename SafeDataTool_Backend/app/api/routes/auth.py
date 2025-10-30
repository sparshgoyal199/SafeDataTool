from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session
from app.db.session import get_session
from app.schemas.user import UserCreate, UserOut, Token
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jwt.exceptions import InvalidTokenError
from app.services.user_service import create_user, login_user

auth_router = APIRouter(prefix="/auth", tags=["auth"])

#have created similar routes for all the distinct features
#it means created names for all these routes for grouping similar routes - that's why we use APIRouter
@auth_router.post("/signup", response_model=UserOut)
def signup(user: UserCreate, session: Session = Depends(get_session)):
    from main import AuthException
    #is it necessary to do exception handling in this fucntion when we have global exception handler for dealing with the exceptions?
    #Use both together smartly:
        #Use local try–except for very specific, predictable issues.
        #
        #Use global handlers for all recurring or framework-level exceptions.
    try:
        #where we raised exceptions from our side by using raise then it will automatically goes to exception block or it can happen that exception may be raised by the python then it will also goes to catch block
        return create_user(user, session)
    except ValueError as e:
        raise AuthException(e.__str__())
    #__str__ helps us to extract the message from the valuerror
    except Exception as e:
        raise HTTPException(402, str(e))
    

@auth_router.post("/signin",response_model=Token)
def signin(form_data: OAuth2PasswordRequestForm = Depends(), session: Session = Depends(get_session)) -> Token:
    from main import AuthException
    try:
        return login_user(form_data.username,form_data.password, session)
    except ValueError as e:
        raise AuthException(e.__str__())
    except Exception as e:
        raise HTTPException(401, str(e))
    