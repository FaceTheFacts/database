import sys

sys.path.append("src")
from query import Query
from fetch import country_fetch


class Country:
    def __init__(self):
        self.new_query = Query()

    def create_table(self):
        sql_command = """CREATE TABLE country (
      id integer PRIMARY KEY,
      entity_type varchar,
      label varchar,
      api_url varchar
    );"""
        return self.new_query.sql_command_execution(sql_command)

    def insert_data(self):
        cities = country_fetch()
        for country in cities:
            self.new_query.sql_command_execution(
                "INSERT INTO country (id, entity_type, label, api_url) VALUES(%s,%s,%s,%s)",
                (
                    country["id"],
                    country["entity_type"],
                    country["label"],
                    country["api_url"],
                ),
            )
        return

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


country = Country()
country.create_table()
country.insert_data()
country.cursor_close()
country.connection_close()