import os
from sqlmodel import Session, SQLModel, create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus
load_dotenv()

DB_USER = os.getenv("DB_USER", "your_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "your_project_db")
DB_PORT = os.getenv("DB_PORT", "5432")

# URL encode the password to handle special characters
encoded_password = quote_plus(DB_PASSWORD)
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=True)

def init_db():
    import db.models.scheme
    print("Database URL:", DATABASE_URL)
    print("Database initialized successfully.")
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session
