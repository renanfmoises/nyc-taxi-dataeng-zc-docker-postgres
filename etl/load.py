""" This module loads the data into the database. """

import io
import datetime
import psycopg2

def copy_from_stringio(df, conn, table):
    """This function copies data from a pandas dataframe to a PostgreSQL table.
    Args:
        df (pandas.DataFrame): dataframe with the data to be copied.
        conn (object): connection to the database.
        table (str): name of the table to be populated.

    Returns:
        None
    """
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()

    try:
        # copy_expert method allows to use COPY FROM STDIN
        cursor.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT CSV)", buffer)
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1


    cursor.close()

def copy_from_pandas(df, conn, table):
    """This function copies data from a pandas dataframe to a PostgreSQL table.
    Args:
        df (pandas.DataFrame): dataframe with the data to be copied.
        conn (object): connection to the database.
        table (str): name of the table to be populated.

    Returns:
        None
    """

    df.to_sql(name = table, con = conn, if_exists = 'append')
