import sys

sys.path.append("src")
from fetch import politician_fetch
from query import Query

columns = [
    "id",
    "entity_type",
    "label",
    "api_url",
    "abgeordnetenwatch_url",
    "first_name",
    "last_name",
    "birth_name",
    "sex",
    "year_of_birth",
    "party_id",
    "party_past",
    "deceased",
    "deceased_date",
    "education",
    "residence",
    "occupation",
    "statistic_questions",
    "statistic_questions_answered",
    "qid_wikidata",
    "field_title",
]


class Politician:
    def __init__(self):
        self.new_query = Query()
        self.table_name = "politician"

    def create_table(self):
        sql_command = """CREATE TABLE {} (
      {} integer PRIMARY KEY,
      {} varchar,
      {} varchar,
      {} varchar,
      {} varchar,
      {} varchar,
      {} varchar,
      {} varchar,
      {} varchar,
      {} integer,
      {} integer NOT NULL REFERENCES party(id),
      {} varchar,
      {} boolean,
      {} date,
      {} varchar,
      {} varchar,
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
            columns[6],
            columns[7],
            columns[8],
            columns[9],
            columns[10],
            columns[11],
            columns[12],
            columns[13],
            columns[14],
            columns[15],
            columns[16],
            columns[17],
            columns[18],
            columns[19],
            columns[20],
        )

        return self.new_query.sql_command_execution(sql_command)

    def insert_data(self):
        politicians = politician_fetch()
        for politician in politicians:
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
            {},
            {},
            {},
            {},
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
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
            
          )""".format(
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
                columns[9],
                columns[10],
                columns[11],
                columns[12],
                columns[13],
                columns[14],
                columns[15],
                columns[16],
                columns[17],
                columns[18],
                columns[19],
                columns[20],
            )
            sql_tuple = (
                politician[columns[0]],
                politician[columns[1]],
                politician[columns[2]],
                politician[columns[3]],
                politician[columns[4]],
                politician[columns[5]],
                politician[columns[6]],
                politician[columns[7]],
                politician[columns[8]],
                politician[columns[9]],
                politician["party"]["id"],
                politician[columns[11]],
                politician[columns[12]],
                politician[columns[13]],
                politician[columns[14]],
                politician[columns[15]],
                politician[columns[16]],
                politician[columns[17]],
                politician[columns[18]],
                politician[columns[19]],
                politician[columns[20]],
            )
            self.new_query.sql_command_execution(sql_string, sql_tuple)
        return

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


politician = Politician()
politician.create_table()
politician.insert_data()
politician.cursor_close()
politician.connection_close()
