#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import warnings
import numpy as np


def clean_data(df, **args):
    """
    Clean data

    Arguments:
        ```
        df: pandas.DataFrame containing invoice data.
        ```

    Details:
        This function cleans data.

    Examples:
        ```
        # Create a dummy data
        data = {"name1": ["FI", "FI", "FI"],
                "päivämäärä": ["02012023", "2-1-2023", "1.1.2023"],
                "name3": [1, 2, 2],
                "org_name": ["Turku", "Turku", "Turku"],
                "supplier": ["Myyjä", "Supplier Oy", "Myyjän tuote Oy"],
                "summa": [100.21, 10.30, 50.50],
                }
        df = pd.DataFrame(data)
        ```

    Output:
        pandas.DataFrame with cleaned data.

    """
    # INPUT CHECK
    # df must be pandas DataFrame
    if not isinstance(df, pd.DataFrame) or\
        df.shape[0] == 0 or\
            df.shape[1] == 0:
        raise Exception(
            "'df' must be non-empty pandas.DataFrame."
            )
    # INPUT CHECK END
    # Check if there are empty rows or columns, and remove them
    if any(df.isna().all(axis=0)) or any(df.isna().all(axis=1)):
        df = df.dropna(axis=0)
        df = df.dropna(axis=1)
        warnings.warn(
            message="'df' contained empty rows or/and columns \n" +
            "that are now removed.\n",
            category=Warning
            )
    # Check if there are duplicated column names
    if len(set(df.columns)) != df.shape[1]:
        # Get unique values and their counts
        unique, counts = np.unique(df.columns, return_counts=True)
        # Get duplicated values
        duplicated = unique[counts > 1]
        # Get those duplicated values that define columns
        # that are being cleaned
        duplicated_disable = duplicated[list(dup in ["test1", "test3"]
                                             for dup in duplicated)]
        warnings.warn(
            message=f"The following column names are duplicated. "
            f"Please check them for errors.\n {duplicated}",
            category=Warning
            )
    # Check org_number
    return df
