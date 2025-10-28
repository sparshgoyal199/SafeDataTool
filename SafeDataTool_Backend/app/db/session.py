# database.py
from sqlmodel import SQLModel, create_engine, Session
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL, echo=True)  # echo=True shows SQL logs

# Function to initialize DB tables
def init_db():
    SQLModel.metadata.create_all(engine)
    
def get_session() -> Session:
    with Session(engine) as session:
        yield session
