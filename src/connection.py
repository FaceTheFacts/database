import os

from dotenv import load_dotenv
import psycopg2


def connect():
    connection = None
    try:
        print("Connecting to the postgreSQL ...")

        load_dotenv()
        database_host = os.getenv("DATABASE_HOST")
        database_user = os.getenv("DATABASE_USER")
        database_password = os.getenv("DATABASE_PASSWORD")

        connection = psycopg2.connect(
            host=database_host,
            database="facethefacts",
            user=database_user,
            password=database_password,
        )
        print("Connected to the facethefacts database")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if connection is not None:
            return connection
