""" This module creates the database's tables. """

import datetime
# import pandas as pd
import sqlalchemy

def create_table(df, user, password, host, port, database, table):
    """This function creates a table in a PostgreSQL database.
    Args:
        conn (object): connection to the database.
        table (str): name of the table to be created.
    Returns:
        None
    """

    engine = sqlalchemy.create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    try:
        df.head(n=0).to_sql(name = table, con = engine, if_exists = 'replace', index = False)
        print(f">> {datetime.datetime.now()} | [ ETL ] | {table} table created.")

    except (Exception, sqlalchemy.exc.ProgrammingError) as error:
        print(f"Error: {error}")
        return 1
