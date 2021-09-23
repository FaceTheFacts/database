from fetch import party_fetch
from query import Query

new_query = Query()


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
        return new_query.sql_command_execution(sql_command)

    def insert_data(self):
        parties = party_fetch()
        for party in parties:
            new_query.sql_command_execution(
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
        return


party = Party()
party.create_table()
party.insert_data()
new_query.cursor_close()
new_query.connection_close()
