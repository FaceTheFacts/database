from connection import connect
import psycopg2


class Query:
    def __init__(self) -> None:
        conn = connect()
        self.conn = conn
        self.cur = conn.cursor()

    def cursor_execute(self, query, *vars):

        return self.cur.execute(query, *vars)

    def cursor_close(self):
        print("Cursor is closed")
        return self.cur.close()

    def connection_commit(self):
        return self.conn.commit()

    def connection_close(self):
        print("Connection is closed")
        return self.conn.close()

    def sql_command_execution(self, query, *vars):
        try:
            self.cursor_execute(query, *vars)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            self.connection_commit()
