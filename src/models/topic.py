import sys

sys.path.append("src")
from query import Query
from fetch import topic_fetch


class Topic:
    def __init__(self):
        self.new_query = Query()

    def create_table(self):
        sql_command = """CREATE TABLE topic (
      id integer PRIMARY KEY,
      entity_type varchar,
      label varchar,
      api_url varchar,
      abgeordnetenwatch_url varchar, 
      description varchar
      );"""
        return self.new_query.sql_command_execution(sql_command)

    def insert_data(self):
        topics = topic_fetch()
        for topic in topics:
            """ if topic['description'] != None: """
            self.new_query.sql_command_execution(
                "INSERT INTO topic (id, entity_type, label, api_url, abgeordnetenwatch_url, description) VALUES(%s,%s,%s,%s,%s,%s)",
                (
                    topic["id"],
                    topic["entity_type"],
                    topic["label"],
                    topic["api_url"],
                    topic["abgeordnetenwatch_url"],
                    topic["description"]
                ),
            )
        return
        """ if topic['description'] == None:
                self.new_query.sql_command_execution(
                    "INSERT INTO topic (id, entity_type, label, api_url, abgeordnetenwatch_url, description) VALUES(%s,%s,%s,%s,%s,%s)",
                    (
                        topic["id"],
                        topic["entity_type"],
                        topic["label"],
                        topic["api_url"],
                        topic["abgeordnetenwatch_url"],
                        None,
                    ),
                )
                return """

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


topic = Topic()
topic.create_table()
topic.insert_data()
topic.cursor_close()
topic.connection_close()