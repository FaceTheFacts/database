import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

database_user = os.getenv("DATABASE_USER")
database_password = os.getenv("DATABASE_PASSWORD")

connection = psycopg2.connect(
    host="localhost",
    database="facethefacts",
    user=database_user,
    password=database_password,
)

cursor = connection.cursor()
cursor.execute("select * from party")
rows = cursor.fetchall()

for row in rows:
    print(f"id {row[0]} display name {row[1]}")

rows = cursor.fetchall()


connection.close()
