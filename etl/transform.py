""" This module contains the functions to transform the data. """

from typing import Union
import pandas as pd

def to_datetime(df, columns: Union[str, list]) -> pd.DataFrame:
    """This function converts a column to datetime.
    Args:
        df (pandas.DataFrame): dataframe with the data.
        columns (str or list[str]): name of the column to be converted.
    Returns:
        df (pandas.DataFrame): dataframe with the data.
    """
    if isinstance(columns, str):
        columns = [columns]

    for column in columns:
        df[column] = pd.to_datetime(df[column])

    return df