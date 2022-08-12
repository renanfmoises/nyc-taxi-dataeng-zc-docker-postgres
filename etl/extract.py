""" This function downloads the parquet file and converts and stores it to csv. """

import os
import pandas as pd


class Extract:
    def __init__(
        self, url, parquet_file_name="output.parquet", csv_file_name="output.csv"
    ):
        self.url = url
        self.parquet_file_name = parquet_file_name
        self.csv_file_name = csv_file_name
        self.__df = None

    def download_parquet(self):
        os.system(f"wget {self.url} -O {self.parquet_file_name}")

    def get_df(self):
        self.__df = pd.read_parquet(self.parquet_file_name)

    def n_rows(self):
        n_rows = self.__df.shape[0]
        return n_rows

    def convert_parquet_to_csv(self):
        self.__df.to_csv(self.csv_file_name)
