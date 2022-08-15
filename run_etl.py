""" This module is the main module of the ETL process. """

import argparse
import datetime
import logging
from time import time

import pandas as pd
import numpy as np

from db_actions.connect import connect_to_postgres
from db_actions.create_table import create_table
from db_actions.drop_table import drop_table

from etl.extract import Extract
from etl.load import copy_from_stringio

from etl.transform import to_datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="logs/run_etl.log",
    filemode="w",
)


def main(params, drop=True):
    """This function executes the ETL process.
    Args:
        params (dict): parameters for the ETL process.
        drop (bool): whether to drop the table before creating it.
        chunks (bool): whether to split the dataframe into chunks.
        chunksize (int): size of the chunks. Only accessed if chunks is True.

    Returns:
        None
    """

    # Connect to the database
    # import ipdb; ipdb.set_trace()
    conn = connect_to_postgres(
        params.user, params.password, params.host, params.port, params.database
    )
    logging.info("Connected to %(database)s database", {"database": params.database})

    # Drop the table if it exists
    if drop:
        drop_table(conn, params.table_name)
        logging.info("Dropped %(table)s", {"table": params.table_name})

    # Extract the data from source URL and save it to parquet file
    extract = Extract(params.url)
    extract.download_file_from_url()
    logging.info(
        "Downloaded file %(file_name)s.%(file_extension)s from %(url)s",
        {
            "file_name": extract.file_name,
            "file_extension": extract.file_extension,
            "url": params.url,
        },
    )

    # import ipdb; ipdb.set_trace()
    # Transform the data into a pandas dataframe
    logging.info("Transforming parquet file to pandas.DataFrame...")
    extract.load_pandas_df()

    # Save the dataframe to a variable
    df = extract.pandas_df
    # Get number of rows
    n_rows = extract.n_rows

    logging.info("Loaded %(n_rows)s rows into memory", {"n_rows": n_rows})

    # Transform the dataframe into a csv file for loading into the database
    if params.url.endswith(".parquet"):
        logging.info("Transforming parquet file to csv...")
        extract.convert_parquet_to_csv()  # output.csv
        logging.info("Transformed parquet file to csv: %(file)s", {"file": params.file})

    # Transform csv date column(s) into datetype objects
    if params.dates:
        to_datetime(df, params.datetime_columns)
        logging.info("Transformed date columns into datetime objects")

    create_table(
        df,
        params.user,
        params.password,
        params.host,
        params.port,
        params.database,
        params.table_name,
    )
    logging.info("Created %(table)s table", {"table": params.table_name})

    if params.chunks:
        df_iter = pd.read_csv(
            f"src_files/csv/{extract.file_name}.csv", chunksize=params.chunksize
        )
        total_n_chunks = int(np.ceil(n_rows / params.chunksize))

        for i, df_chunk in enumerate(df_iter):

            t_start = time()

            copy_from_stringio(df=df_chunk, conn=conn, table=params.table_name)

            t_end = time()
            print(
                f">> {datetime.datetime.now()} | [ ETL ] | Chunk {i + 1} of {total_n_chunks} chunks loaded into databse in {round(t_end - t_start, 2)} seconds."
            )
            logging.info(
                "Chunk %(i)s of %(total_n_chunks)s chunks loaded into databse in %(t_end)s seconds...",
                {
                    "datetime": datetime.datetime.now(),
                    "i": i + 1,
                    "total_n_chunks": total_n_chunks,
                    "t_end": round(t_end - t_start, 2),
                },
            )

    else:
        t_start = time()

        copy_from_stringio(df=df, conn=conn, table=params.table_name)

        t_end = time()
        print(
            f">> {datetime.datetime.now()} | [ ETL ] | Loaded {n_rows} rows into database in {round(t_end - t_start, 2)} seconds."
        )
        logging.info(
            "Loaded %(n_rows)s into databse in %(seconds)s seconds...",
            {"n_rows": n_rows, "seconds": round(t_end - t_start, 2)},
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ETL script for loading data into a PostgreSQL database."
    )

    parser.add_argument("-u", "--user", help="Database user", required=True)
    parser.add_argument("-p", "--password", help="Database password", required=True)
    parser.add_argument("-H", "--host", help="Database host", required=True)
    parser.add_argument("-P", "--port", help="Database port", required=True)
    parser.add_argument("-D", "--database", help="Database name", required=True)
    parser.add_argument(
        "-t", "--table_name", help="Table to ingest into", required=True
    )
    parser.add_argument("-U", "--url", help="URL to download data from", required=True)
    parser.add_argument("-f", "--file", help="File to ingest", required=False)
    parser.add_argument(
        "-d",
        "--dates",
        help="Dataset contains date columns",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--datetime_columns",
        help="Columns to convert to datetime",
        nargs="*",
        required=False,
    )
    parser.add_argument(
        "--chunks",
        help="Whether to split the dataframe into chunks",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--chunksize", help="Size of the chunks", type=int, required=False
    )

    args = parser.parse_args()

    main(args, drop=True)
    # main(args, drop = False)
