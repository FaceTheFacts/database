from connection import connect
import psycopg2


class Query:
    def __init__(self) -> None:
        conn = connect()
        self.conn = conn
        self.cur = conn.cursor()

    def cursor_close(self):
        print("Cursor is closed")
        return self.cur.close()

    def connection_commit(self):
        return self.conn.commit()

    def connection_close(self):
        print("Connection is closed")
        return self.conn.close()

    def sql_command_execution(self, query: str, *value: tuple):
        try:
            self.cur.execute(query, *value)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            self.connection_commit()
