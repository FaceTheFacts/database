import os
from dotenv import load_dotenv
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()
database_user = os.getenv("DATABASE_USER")
database_host = os.getenv("DATABASE_HOST")
database_password = os.getenv("DATABASE_PASSWORD")

connection_uri = sa.engine.URL.create(
   "postgresql+psycopg2",
    username=database_user,
    password=database_password,
    host=database_host,
    database="facethefacts",
)

Base = declarative_base()

engine = create_engine(connection_uri)

Session = sessionmaker(bind=engine)
