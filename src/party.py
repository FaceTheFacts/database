from connection import connect

conn = connect()
cur = conn.cur()


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

        cur.execute(sql_command)

        conn.commit()
        cur.close()
        conn.close()


party = Party()
party.create_table()
