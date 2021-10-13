import os
from dotenv import load_dotenv
import sqlalchemy

load_dotenv()

database_user = os.getenv("DATABASE_USER")
database_host = os.getenv("DATABASE_HOST")
database_password = os.getenv("DATABASE_PASSWORD")

connection_uri = sqlalchemy.engine.URL.create(
    "postgresql+psycopg2",
    username=database_user,
    password=database_password,
    host=database_host,
    database="facethefacts",
)
