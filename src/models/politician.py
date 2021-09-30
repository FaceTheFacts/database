import sys

sys.path.append("src")
from fetch import politician_fetch
from query import Query


class Politician:
    def __init__(self):
        self.new_query = Query()
        self.table_name = "politician"
        self.columns = [
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

    def create_table(self):
        sql_command = """
        CREATE TABLE {} (
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
            self.table_name, *self.columns
        )

        return self.new_query.sql_command_execution(sql_command)

    def populate_table(self):
        api_politicians = politician_fetch()
        politician_list = []
        api_fields = self.columns.copy()
        api_fields[10] = "party"

        for api_politician in api_politicians:
            party = api_politician["party"]
            politician_values = list(map(lambda k: api_politician[k], api_fields))
            politician_values[10] = api_politician["party"]["id"] if party else None
            politician_list.append(tuple(politician_values))

        query_str = self.new_query.generate_query_str(politician_list)
        args = ", ".join(self.columns)
        query_cmd = f"INSERT INTO {self.table_name} ({args}) VALUES "
        self.new_query.sql_command_execution(query_cmd + query_str)

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


politician = Politician()
politician.create_table()
politician.populate_table()
politician.cursor_close()
politician.connection_close()
