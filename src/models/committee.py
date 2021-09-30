import sys

sys.path.append("src")
from query import Query
from fetch import committee_fetch


class Committee:
    def __init__(self):
        self.new_query = Query()

    def create_table(self):
        sql_command = """CREATE TABLE committee (
      id integer PRIMARY KEY,
      entity_type varchar,
      label varchar,
      api_url varchar,
      field_legislature_id integer REFERENCES parliament_period(id)
      );"""
        return self.new_query.sql_command_execution(sql_command)

    def create_bridging_table(self):
        sql_command = """CREATE TABLE committee_has_topics (
      id integer PRIMARY KEY,
      committee_id integer REFERENCES committee(id),
      topic_id integer REFERENCES topic(id)
      );"""
        return self.new_query.sql_command_execution(sql_command) 
    def insert_data(self):
        committees = committee_fetch()
        i = 0
        for committee in committees:
            """Run this for creating the table"""
            """ self.new_query.sql_command_execution(
                "INSERT INTO committee (id, entity_type, label, api_url, field_legislature_id) VALUES(%s,%s,%s,%s,%s)",
                (
                    committee["id"],
                    committee["entity_type"],
                    committee["label"],
                    committee["api_url"],
                    committee["field_legislature"]["id"]
                ),
            ) """
            """Run this for inserting data in the bridging table"""
            """if committee['field_topics'] != None:
                for topic in committee["field_topics"]:
                    self.new_query.sql_command_execution(
                        "INSERT INTO committee_has_topics (id, committee_id, topic_id) VALUES(%s,%s,%s)",
                        (
                            i,
                            committee["id"],
                            topic["id"]
                        ),
                    )
                    i = i + 1"""
            "Not sure if this makes sense but I want to add the field_topics column to the committee table. There might be no need for this column though."
            if committee["field_topic"] != None:
                sql_command = "UPDATE committee SET field_topic_id={topic} WHERE id={committee_id}".format(topic=committee["field_topics"]["id"], committee_id=committee["id"]) 
                self.new_query.sql_command_execution(sql_command)
        return

    def alter_table(self):
        sql_command = """ALTER committee 
      ADD COLUMN field_topic_id integer REFERENCES committee_has_topics(topic_id)"""
        return self.new_query.sql_command_execution(sql_command)

    def cursor_close(self):
        return self.new_query.cursor_close()

    def connection_close(self):
        return self.new_query.connection_close()


committee = Committee()
"""Depending on the step: change alter_table to create_table or create_bridging_table. Change accordingly the insert_data function"""
committee.alter_table()
committee.insert_data()
committee.cursor_close()
committee.connection_close()
