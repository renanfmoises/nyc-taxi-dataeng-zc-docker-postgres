""" This function downloads the parquet file and converts and stores it to csv. """

from dataclasses import dataclass
import os
import pandas as pd

@dataclass
class Extract:
    url: str
    parquet_file_name: str = "output.parquet"
    csv_file_name: str = "output.csv"
    df = None
    n_rows = None

    def download_parquet(self):
        """ Download the parquet file and converts and stores it to csv. """
        os.system(f"wget {self.url} -O {self.parquet_file_name}")

    def get_pandas_df(self):
        """ Load the pandas dataframe into memmory and update n_rows attribute. """
        self.df = pd.read_parquet(self.parquet_file_name)
        self.n_rows = self.df.shape[0]
        return self.df, self.n_rows

    def convert_parquet_to_csv(self):
        """ Convert the parquet file to csv. """
        self.df.to_csv(self.csv_file_name, index=False, header=True)
