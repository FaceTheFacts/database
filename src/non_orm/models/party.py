import sys

sys.path.append("src")
from fetch import party_fetch
from query import Query


class Party:
    def __init__(self):
        self.new_query = Query()

    def create_table(self):
        sql_command = """CREATE TABLE party (
      id integer PRIMARY KEY,
      entity_type varchar,
      label varchar,
      api_url varchar,
      full_name varchar,
      short_name varchar
    );"""
        return self.new_query.sql_command_execution(sql_command)

    def insert_data(self) -> None:
        party_list = party_fetch()
        for party in party_list:
            self.new_query.sql_command_execution(
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

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


party = Party()
party.create_table()
party.insert_data()
party.cursor_close()
party.connection_close()
