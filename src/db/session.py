from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from .connection import connection_uri


engine = create_engine(connection_uri)
Session = sessionmaker(bind=engine)
