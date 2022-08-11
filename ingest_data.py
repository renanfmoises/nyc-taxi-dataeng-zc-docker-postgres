""" This module is the main module for the ingest_data.py script. It takes in the parameters and then calls the other functions."""

# TODO: Change module to Class
# TODO: Change pandas.DataFrame iterable to spark.DataFrame iterable
# TODO: Get number of chunks to loop through

import argparse
import logging
import os
from time import time

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="ingest_data.log",
    filemode="w",
)


# Create engine and return it
def get_engine(user, password, host, port, database):
    """This function returns an engine for the database.

    Args:
        user (str): Postgres username
        password (str): Postgres password
        host (str): Postgres host
        port (str): Postgres port
        database (str): Postgres database

    Returns:
        engine (sqlalchemy.engine.Engine): Engine for the database
    """

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    return engine


# Drop table if exists
def drop_table(user, password, host, port, database, table_name):
    """This function drops a table from the database.

    Args:
        user (str): Postgres username
        password (str): Postgres password
        host (str): Postgres host
        port (str): Postgres port
        database (str): Postgres database

    Returns:
        None
    """

    engine = get_engine(user, password, host, port, database)
    engine.execute(f"DROP TABLE IF EXISTS {table_name};")


# Get csv data
def get_csv_data(url, parquet_file_name="output.parquet", csv_file_name="output.csv"):
    """This function downloads a csv file from a url and saves it to a csv file.

    Args:
        url (str): URL to download the csv file from
        parquet_file_name (str): Name of the parquet file to load into memory, and then convert to csv
        csv_file_name (str): Name of the csv file to save to
    Returns:
        None
    """
    os.system(f"wget {url} -O {parquet_file_name}")
    df = pd.read_parquet(parquet_file_name)
    df.to_csv(csv_file_name)


# Load csv data into database
def main(params):
    """This function is the main function for the ingest_data.py script. It takes in the parameters and then calls the other functions."""

    # DB connection parameters
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database

    # Other parameters
    table_name = params.table_name
    url = params.url
    file = params.file
    csv_file_name = "output.csv"

    # Download/Get csv data
    get_csv_data(url=url, csv_file_name=csv_file_name)
    logging.info("Downloaded %(file)s", {"file": csv_file_name})

    # Create engine
    engine = get_engine(user, password, host, port, database)
    logging.info("Created engine for %(database)s", {"database": database})

    # Drop table if exists
    drop_table(user, password, host, port, database, table_name)
    logging.info("Dropped table %(table_name)s", {"table_name": table_name})

    # Dump csv to postgres
    ## Get info for logging
    chunksize = 131_072
    # n_chunks_total = np.ceil(os.system(f"wc -l {csv_file_name}").split(" ")[0].astype(int) / chunksize)

    ## Create the DataFrame iterator
    df_iter = pd.read_csv(csv_file_name, iterator=True, chunksize=chunksize)
    logging.info("Created iterator for %(file)s", {"file": csv_file_name})

    ## Loop through each DataFrame in the iterator
    for i, df_chunk in enumerate(df_iter):

        t_start = time()

        # Change date columns to datetime type objects
        df_chunk["tpep_pickup_datetime"] = pd.to_datetime(
            df_chunk["tpep_pickup_datetime"]
        )
        df_chunk["tpep_dropoff_datetime"] = pd.to_datetime(
            df_chunk["tpep_dropoff_datetime"]
        )

        # Append chunk to the database
        df_chunk.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        logging.info(
            "Ingested chunk %(i)s to %(table_name)s in %(time)s seconds",
            {"i": i + 1, "table_name": table_name, "time": round((t_end - t_start), 2)},
        )

    logging.info("Finished ingestion of %(file)s to %(table_name)s", {"file": csv_file_name, "table_name": table_name})

# Parse arguments and call main function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data into database")
    parser.add_argument("-d", "--database", help="Database name", required=True)
    parser.add_argument("-u", "--user", help="Database user", required=True)
    parser.add_argument("-p", "--password", help="Database password", required=True)
    parser.add_argument("-H", "--host", help="Database host", required=True)
    parser.add_argument("-P", "--port", help="Database port", required=True)
    parser.add_argument("-U", "--url", help="URL to download data from", required=True)
    parser.add_argument("-f", "--file", help="File to ingest", required=True)
    parser.add_argument(
        "-t", "--table_name", help="Table to ingest into", required=True
    )

    args = parser.parse_args()

    main(args)
