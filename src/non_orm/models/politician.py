import sys
import json

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
      {} integer REFERENCES party(id),
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
            self.table_name, *columns
        )

        return self.new_query.sql_command_execution(sql_command)

    def insert_data(self):
        politicians = politician_fetch()
        no_party_politicians = []
        for politician in politicians:
            id = politician.get("id")
            party = politician.get("party")
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

                ) ON CONFLICT(id) DO NOTHING""".format(
                self.table_name,
                *columns,
            )

            politician_values = list(map(lambda k: politician[k], columns))

            if party is None:
                print("ID: No.{id} politician doesn't have a party".format(id=id))
                politician_values[10] = politician["party"]
            else:
                politician_values[10] = politician["party"]["id"]

            sql_tuple = tuple(politician_values)

            self.new_query.sql_command_execution(sql_string, sql_tuple)

        with open("no_party_politicians.json", "w", encoding="utf-8") as file:
            json.dump(no_party_politicians, file, ensure_ascii=False, indent=4)
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
