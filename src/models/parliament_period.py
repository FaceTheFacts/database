import sys

sys.path.append("src")
from fetch import parliament_period_fetch
from query import Query

columns = [
    "id",
    "entity_type",
    "label",
    "api_url",
    "abgeordnetenwatch_url",
    "type",
    "election_date",
    "start_date_period",
    "end_date_period",
]


class Parliament_period:
    def __init__(self):
        self.new_query = Query()
        self.table_name = "parliament_period"

    def create_table(self):
        sql_command = """CREATE TABLE {} (
      {} integer PRIMARY KEY,
      {} varchar,
      {} varchar,
      {} varchar,
      {} varchar,
      {} varchar,
      {} date,
      {} date,
      {} date

    );""".format(
            self.table_name,
            columns[0],
            columns[1],
            columns[2],
            columns[3],
            columns[4],
            columns[5],
            columns[6],
            columns[7],
            columns[8],
        )

        return self.new_query.sql_command_execution(sql_command)

    def insert_data(self):
        parliament_periods = parliament_period_fetch()
        for parliament_period in parliament_periods:
            sql_string = """INSERT INTO {} 
            (
                {},
                {},
                {},
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
                columns[6],
                columns[7],
                columns[8],
            )
            sql_tuple = (
                parliament_period[columns[0]],
                parliament_period[columns[1]],
                parliament_period[columns[2]],
                parliament_period[columns[3]],
                parliament_period[columns[4]],
                parliament_period[columns[5]],
                parliament_period[columns[6]],
                parliament_period[columns[7]],
                parliament_period[columns[8]],
            )
            self.new_query.sql_command_execution(sql_string, sql_tuple)

        return

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


parliament_period = Parliament_period()
parliament_period.create_table()
parliament_period.insert_data()
parliament_period.cursor_close()
parliament_period.connection_close()
