""" This module stablishes a connection to a PostgreSQL database. """

import sys
import datetime
import psycopg2

def connect_to_postgres(user, password, host, port, database):
    """This function connects to a PostgreSQL database.
    Args:
        user (str): user name.
        password (str): password.
        host (str): host name.
        port (str | int): port number.
        database (str): database name.

    Returns:
        conn (object): pyscopg2 connection to the PostgreSQL database.
    """

    conn = None

    try:
        # connect to the PostgreSQL database
        print(
            f">> {datetime.datetime.now()} | [ CONN ] | Connecting to PostgreSQL database..."
        )

        conn = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,
            host=host,
            port=port,
        )

        print(
            f">> {datetime.datetime.now()} | [ CONN ] | Connection to PostgreSQL database successful."
        )
        return conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        sys.exit(1)

