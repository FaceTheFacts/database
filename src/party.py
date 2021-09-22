from connection import connect
import psycopg2

conn = connect()
cur = conn.cursor()


class Party:
    def create_table(self):
        sql_command = """CREATE TABLE party (
      id integer PRIMARY KEY,
      entity_type varchar,
      label varchar,
      api_url varchar,
      full_name varchar,
      short_name varchar
    );"""
        try:
            cur.execute(sql_command)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            conn.close()


party = Party()
party.create_table()
