import sys

sys.path.append("src")
from query import Query
from fetch import city_fetch


class City:
    def __init__(self):
        self.new_query = Query()

    def create_table(self):
        sql_command = """CREATE TABLE city (
      id integer PRIMARY KEY,
      entity_type varchar,
      label varchar,
      api_url varchar
    );"""
        return self.new_query.sql_command_execution(sql_command)

    def insert_data(self):
        cities = city_fetch()
        for city in cities:
            self.new_query.sql_command_execution(
                "INSERT INTO city (id, entity_type, label, api_url) VALUES(%s,%s,%s,%s)",
                (
                    city["id"],
                    city["entity_type"],
                    city["label"],
                    city["api_url"],
                ),
            )
        return

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


city = City()
city.create_table()
city.insert_data()
city.cursor_close()
city.connection_close()
