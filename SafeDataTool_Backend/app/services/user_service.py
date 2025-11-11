# services/user_service.py
from sqlmodel import Session, select, or_
from app.db.models import User, OTPVerification, PendingUser
from app.schemas.user import UserCreate, UserOut, Token, OTP, Password
import smtplib
import os
import random
from email.message import EmailMessage
from datetime import datetime
from app.core.security import hash_password, create_access_token
from dotenv import load_dotenv

load_dotenv() 

def sendingOtp(email, signup_otp):
    FROM_EMAIL = os.getenv('SMTP_EMAIL')
    SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
    TO_EMAIL = email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(FROM_EMAIL, SMTP_PASSWORD)
    msg = EmailMessage()
    msg['Subject'] = 'OTP Verification'
    msg['From'] = FROM_EMAIL
    msg['To'] = TO_EMAIL
    msg.set_content('YOUR OTP is: ' + signup_otp)
    server.send_message(msg)
    
def generatingOTP():
    signup_otp = ""
    for i in range(6):
        signup_otp += str(random.randint(0, 9))
    return signup_otp

def otp_handling(email: str) -> OTPVerification:
    user_email = email
    signup_otp = generatingOTP()
    sendingOtp(user_email, signup_otp)
    otp_instance = OTPVerification(email=user_email,otp=signup_otp)
    return otp_instance

def create_temporary_user(username: str,email: str, hashed_password: str,session: Session):
    # user_email = email
    # signup_otp = generatingOTP()
    # sendingOtp(user_email, signup_otp)
    # otp_instance = OTPVerification(email=user_email,otp=signup_otp)
    otp_instance = otp_handling(email)
    pending_user = PendingUser(username=username,email=email,password=hashed_password,otp=otp_instance)
    session.add(pending_user)
    session.commit()
    session.refresh(pending_user)
    return OTP(message = f"OTP sent to your email {email} for verification",status="pending_verification")

# def validate_user(user_in: UserCreate,session: Session) -> OTP:
#     # Validate & hash
#     already_user = session.exec(select(User).where(User.username == user_in.username)).first()
#     # already_user = session.exec(select(User).where(or_(User.username == user_in.username,User.email == user_in.email))).first()
#     if already_user:
#         raise ValueError("username or email already exist")
#     hashed_password = hash_password(user_in.password)
#     return otp_store(user_in.username,user_in.email,hashed_password,session)

def login_user(username: str,password: str,session: Session) -> Token:
    # Validate & hash
    hashed_password = hash_password(password)
    db_user = session.exec(select(User).where(User.username == username,User.password == hashed_password)).first()
    if not db_user:
        raise ValueError("please enter correct username or password")
    access_token = create_access_token({"sub": db_user.id})
    return Token(access_token=access_token, token_type="bearer")

def otp_verification(otp: str,session: Session) -> OTPVerification:
    otp_exist = session.exec(select(OTPVerification).where(otp == OTPVerification.otp,OTPVerification.expires_at > datetime.utcnow())).first()
    if not otp_exist:
        raise ValueError('otp is invalid')
    invalid_otp = session.exec(select(OTPVerification).where(OTPVerification.expires_at < datetime.utcnow())).all() 
    for i in invalid_otp:
        session.delete(i)
    return otp_exist

def create_permanent_user(otp: str,session: Session) -> UserOut:
    otp_exist = otp_verification(otp,session)
    userpending_instance = otp_exist.pendinguser[0]
    #because of session.delete memory of otp_exist in python will also be deleted
    is_user_exist = session.exec(select(User).where(User.username == userpending_instance.username)).first()
    if is_user_exist:
        raise ValueError('username or email already exist')
    db_user = User(username=userpending_instance.username,email=userpending_instance.email,password=userpending_instance.password)
    session.add(db_user)
    session.delete(otp_exist)
    session.commit()
    session.refresh(db_user)

    return UserOut.model_validate(db_user)  # Or UserOut.model_validate(db_user)

def otp_resend(email: str,session: Session):
    #it will successfully run if he enters its otp within 5 minutes
    pending_user = session.exec(select(PendingUser).where(email == PendingUser.email)).first()
    if not pending_user:
        raise ValueError('Please sign-up again')
    #it is running because i want to extract its temporary data and then creating new record
    return create_temporary_user(pending_user.username,pending_user.email,pending_user.password,session)
    
def forgotpassword_otp(email: str,session: Session):
    is_user = session.exec(select(User).where(User.email == email)).first()
    if not is_user:
        raise ValueError('Email does not exist')
    otp_instance = otp_handling(email)
    session.add(otp_instance)
    session.commit()
    session.refresh(otp_instance)
    return OTP(message = f"OTP sent to your email {email} for verification",status="pending_verification")

def forgotpasword_otp_verification(otp: str,session: Session):
    otp_exist = otp_verification(otp,session)
    delete_all_emails = session.exec(select(OTPVerification).where(OTPVerification.email == otp_exist.email)).all()
    for i in delete_all_emails:
        session.delete(i)
    #session.delete(otp_exist)
    session.commit()
    return "password can be change"

def change_password(password:Password,session: Session):
    user_data = session.exec(select(Password).where(password.email == User.email)).first()
    hashed_password = hash_password(password.password)
    if user_data.password == hashed_password:
        raise ValueError("Please change the password")
    user_data.password = hashed_password
    session.add(user_data)
    session.commit()
    session.refresh(user_data)
    return "Password changed"