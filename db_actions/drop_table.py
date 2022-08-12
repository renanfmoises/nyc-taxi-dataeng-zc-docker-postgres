import datetime
import psycopg2


def drop_table(conn, table):
    """This function drops a table from a PostgreSQL database.
    Args:
        conn (object): connection to the database.
        table (str): name of the table to be dropped.
    Returns:
        None
    """
    cursor = conn.cursor()

    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1

    print(f">> {datetime.datetime.now()} | [ ETL ] | {table} table dropped.")
    cursor.close()
