""" This function downloads the parquet file and converts and stores it to csv. """

from dataclasses import dataclass
import os
import pandas as pd

# @dataclass
class Extract:
    """ This method extracts the data from the source URL and stores it locally.

    Args:
        url (str): URL of the source data.
        file_name (str): name of the file to download.

    Returns:
        None
    """
    def __init__(self, url: str):
        self.__accepted_extensions = ["csv", "parquet"]
        self.__url = url
        self.__file = self.__url.split("/")[-1]

        self.file_name, self.file_extension = self.__file.split(".")
        self.pandas_df = None
        self.n_rows = None


    def download_file_from_url(self) -> None:
        """ Download the source file. """

        if self.file_extension in self.__accepted_extensions:
            os.system(f"wget {self.__url} -O src_files/{self.file_extension}/{self.file_name}.{self.file_extension}")

        else:
            raise ValueError(f"File extension {self.file_extension} is not supported. Supported extensions are {self.__accepted_extensions}")


    def load_pandas_df(self) -> None:
        """ Load the pandas dataframe into memmory and update n_rows attribute. """
        if self.file_extension == "csv":
            self.pandas_df = pd.read_csv(f"src_files/csv/{self.file_name}.csv")

        elif self.file_extension == "parquet":
            self.pandas_df = pd.read_parquet(f"src_files/parquet/{self.file_name}.parquet")
            self.convert_parquet_to_csv()

        self.n_rows = self.pandas_df.shape[0]


    def convert_parquet_to_csv(self) -> None:
        """ Convert the parquet file to csv. """
        self.pandas_df.to_csv(f"src_files/csv/{self.file_name}.csv", index = False)
