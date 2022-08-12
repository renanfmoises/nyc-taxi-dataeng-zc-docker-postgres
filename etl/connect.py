import sys
import datetime
import psycopg2


class ConnectPostgreSQL:
    def __init__(self, user, password, host, port, database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

        self.conn = None

    def connect(self):
        try:
            # connect to the PostgreSQL server
            print(
                f">> {datetime.datetime.now()} | [ CONN ] | Connecting to PostgreSQL database..."
            )

            self.conn = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )

            print(
                f">> {datetime.datetime.now()} | [ CONN ] | Connection to PostgreSQL database successful."
            )
            return self.conn

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            sys.exit(1)
