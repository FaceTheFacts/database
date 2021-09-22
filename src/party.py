from connection import connect
import psycopg2

from fetch import party_fetch

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

    def insert_data(self):
        parties = party_fetch()
        try:
            for party in parties:
                cur.execute(
                    "INSERT INTO party (id, entity_type, label, api_url, full_name, short_name) VALUES(%s,%s,%s,%s,%s,%s)",
                    (
                        party["id"],
                        party["entity_type"],
                        party["label"],
                        party["api_url"],
                        party["full_name"],
                        party["short_name"],
                    ),
                )
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            conn.commit()
            cur.close()
            conn.close()


party = Party()
# party.create_table()
print(party.insert_data())
