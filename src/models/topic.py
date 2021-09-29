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
            "Run this for creating the table"
            self.new_query.sql_command_execution(
                "INSERT INTO topic (id, entity_type, label, api_url, abgeordnetenwatch_url, description) VALUES(%s,%s,%s,%s,%s,%s)",
                (
                    topic["id"],
                    topic["entity_type"],
                    topic["label"],
                    topic["api_url"],
                    topic["abgeordnetenwatch_url"],
                    topic["description"],
                ),
            )
            "Run this when you have created the table because parents relates to the topic id. Parent can be undefined, therefore we have to check it."
            """ if topic["parent"] != None:
                sql_command = "UPDATE topic SET parent={parent} WHERE id={topic_id}".format(parent=topic["parent"][0]["id"], topic_id=topic["id"]) 
                self.new_query.sql_command_execution(sql_command)
            else:
               sql_command = "UPDATE topic SET parent=NULL WHERE id={topic_id}".format(topic_id=topic["id"]) 
               self.new_query.sql_command_execution(sql_command)  """
        return

    def alter_table(self):
        sql_command = """ALTER topic
      ADD COLUMN parent integer REFERENCES topic(id)"""
        return self.new_query.sql_command_execution(sql_command)

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


topic = Topic()
topic.alter_table()
topic.insert_data()
topic.cursor_close()
topic.connection_close()
