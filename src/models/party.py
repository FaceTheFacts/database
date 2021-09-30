import sys

sys.path.append("src")
from fetch import party_fetch
from query import Query


class Party:
    def __init__(self):
        self.new_query = Query()
        self.columns = [
            "id",
            "entity_type",
            "label",
            "api_url",
            "full_name",
            "short_name",
        ]

    def create_table(self):
        sql_command = """
        CREATE TABLE party (
            id integer PRIMARY KEY,
            entity_type varchar,
            label varchar,
            api_url varchar,
            full_name varchar,
            short_name varchar
        );"""
        return self.new_query.sql_command_execution(sql_command)

    def populate_table(self):
        api_parties = party_fetch()
        party_list = []

        for api_party in api_parties:
            party_tuple = tuple(map(lambda k: api_party[k], self.columns))
            party_list.append(party_tuple)

        query_str = self.new_query.generate_query_str(party_list)
        self.new_query.sql_command_execution(
            "INSERT INTO party (id, entity_type, label, api_url, full_name, short_name) VALUES "
            + query_str
        )

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


party = Party()
party.create_table()
party.populate_table()
party.cursor_close()
party.connection_close()
