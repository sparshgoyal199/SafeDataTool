from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session
from app.db.session import get_session
from app.schemas.user import UserCreate, UserOut, Token, OTP, Password
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jwt.exceptions import InvalidTokenError
from app.services.user_service import create_temporary_user, login_user, create_permanent_user, otp_resend, forgotpassword_otp, otp_verification, forgotpasword_otp_verification, change_password
from app.core.security import hash_password
from pydantic import EmailStr

auth_router = APIRouter(prefix="/auth", tags=["auth"])

#have created similar routes for all the distinct features
#it means created names for all these routes for grouping similar routes - that's why we use APIRouter
@auth_router.post("/signup", response_model=OTP)
def signup(user: UserCreate, session: Session = Depends(get_session)):
    from main import AuthException
    #is it necessary to do exception handling in this fucntion when we have global exception handler for dealing with the exceptions?
    #Use both together smartly:
        #Use local try–except for very specific, predictable issues.
        #
        #Use global handlers for all recurring or framework-level exceptions.
    try:
        #where we raised exceptions from our side by using raise then it will automatically goes to exception block or it can happen that exception may be raised by the python then it will also goes to catch block
        return create_temporary_user(user.username,user.email,hash_password(user.password), session)
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
    
    
@auth_router.post("/signup_verifying_otp/{otp}",response_model=UserOut)
def verifying_otp(otp: str,session: Session = Depends(get_session)):
    from main import AuthException
    try:
        return create_permanent_user(otp,session)
    except ValueError as e:
        raise AuthException(e.__str__())
    except Exception as e:
        raise HTTPException(401, str(e))
    
@auth_router.post("/signup_resend_otp/{email}")
def resend_otp(email: str,session: Session = Depends(get_session)):
    from main import AuthException
    try:
        return otp_resend(email,session)
    except ValueError as e:
        raise AuthException(e.__str__())
    except Exception as e:
        raise HTTPException(401, str(e))
    
@auth_router.post("/forgotpassword_email/{email}")
def forgotpassword_email(email: EmailStr,session: Session = Depends(get_session)):
    from main import AuthException
    try:
        return forgotpassword_otp(email,session)
    except ValueError as e:
        raise AuthException(e.__str__())
    except Exception as e:
        raise HTTPException(401, str(e))
    
@auth_router.post("/forgotpassword_verifying_otp/{otp}")
def forgotpassword_verifying_otp(otp: str,session: Session = Depends(get_session)):
    from main import AuthException
    try:
        return forgotpasword_otp_verification(otp,session)
    except ValueError as e:
        raise AuthException(e.__str__())
    except Exception as e:
        raise HTTPException(401, str(e))
    
@auth_router.post("/forgotpassword_resend_otp/{email}")
def forgotpassword_resend_otp(email: str,session: Session = Depends(get_session)):
    from main import AuthException
    try:
        return forgotpassword_otp(email,session)
    except ValueError as e:
        raise AuthException(e.__str__())
    except Exception as e:
        raise HTTPException(401, str(e))
    
@auth_router.post("/password_change")
def password_change(password:Password,session: Session = Depends(get_session)):
    from main import AuthException
    try:
        return change_password(password,session)
    except ValueError as e:
        raise AuthException(e.__str__())
    except Exception as e:
        raise HTTPException(401, str(e))