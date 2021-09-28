import sys
import json

sys.path.append("src")
from fetch import parliament_fetch
from query import Query

columns = [
    "id",
    "entity_type",
    "label",
    "api_url",
    "abgeordnetenwatch_url",
    "label_external_long",
]


class Parliament:
    def __init__(self):
        self.new_query = Query()
        self.table_name = "parliament"

    def create_table(self):
        sql_command = """CREATE TABLE {} (
      {} integer PRIMARY KEY,
      {} varchar,
      {} varchar,
      {} varchar,
      {} varchar,
      {} varchar
    );""".format(
            self.table_name,
            columns[0],
            columns[1],
            columns[2],
            columns[3],
            columns[4],
            columns[5],
        )

        return self.new_query.sql_command_execution(sql_command)

    def insert_data(self):
        parliaments = parliament_fetch()
        for parliament in parliaments:
            sql_string = """INSERT INTO {} 
            (
                {},
                {},
                {},
                {},
                {},
                {}
            ) VALUES (
                %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s               
                ) ON CONFLICT(id) DO NOTHING""".format(
                self.table_name,
                columns[0],
                columns[1],
                columns[2],
                columns[3],
                columns[4],
                columns[5],
            )
            sql_tuple = (
                parliament[columns[0]],
                parliament[columns[1]],
                parliament[columns[2]],
                parliament[columns[3]],
                parliament[columns[4]],
                parliament[columns[5]],
            )
            self.new_query.sql_command_execution(sql_string, sql_tuple)

        return

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


parliament = Parliament()
parliament.create_table()
parliament.insert_data()
parliament.cursor_close()
parliament.connection_close()
