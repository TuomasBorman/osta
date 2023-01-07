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
        print(duplicated_disable)
        warnings.warn(
            message=f"The following column names are duplicated. "
            f"Please check them for errors.\n {duplicated}",
            category=Warning
            )
    # Check org_number
    
    return df


def __clean_sums(df):
    """
    This function checks that sums (total, vat, netsum) are in float format,
    and tries to convert them if they are not. Futhermore, if one field is
    missing, it is calculated based on others.
    Input: df
    Output: df
    """
    # TODO CHECK IF VAT CATEGORY CAN BE FOUND --> VAT COULD BE POSSIBLE
    # TO CALCULATE

    # Check which column is missing if any
    col_to_check = ["total", "vat_amount", "price_ex_vat"]
    # Get columns that are included in data
    col_found = (n if n in df.columns else None for n in col_to_check)
    # Remove Nones
    col_found = [x for x in col_found if x is not None]
    # Get columns that are missing from the data
    col_missing = set(col_to_check).difference(col_found)

    # If data is not float, try to make it as float
    if len(col_found) > 0 and not all(df.dtypes[col_found] == "float64"):
        # Get those column names that need to be modified
        col_not_float = df.dtypes[col_found][
            df.dtypes[col_found] != "float64"].index
        # Loop throug columns
        for col in col_not_float:
            # Replace "," with "." and remove spaces
            df[col] = df[col].str.replace(
                ",", ".").str.split().str.join("")
            # Try to convert values as float
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                warnings.warn(
                    message=f"The following column cannot be converted into "
                    f"float: {col}",
                    category=Warning
                    )
    # If there were some columns missing, calculate them
    if len(col_missing) > 0 and all(df.dtypes[col_found] == "float64"):
        # If total is missing
        if "total" in col_missing and all(c in col_found
                                          for c in ["price_ex_vat",
                                                    "vat_amount"]):
            df["test"] = df["price_ex_vat"] + df["vat_amount"]
        # If price_ex_vat is missing
        elif "price_ex_vat" in col_missing and all(c in col_found
                                                   for c in ["total",
                                                             "vat_amount"]):
            df["price_ex_vat"] = df["total"] - df["vat_amount"]
        # If vat_amount is missing
        elif "vat_amount" in col_missing and all(c in col_found
                                                 for c in ["total",
                                                           "price_ex_vat"]):
            df["vat_amount"] = df["total"] - df["price_ex_vat"]
    return df


def __standardize_dates(df, **args):
    """
    This function converts dates into same format.
    Input: df
    Output: df
    """
    # Get date column
    col = df.loc[:, "date"]
    # Get date format
    date_format = __get_date_format(col, **args)
    day_first = date_format[0]
    year_first = date_format[1]


def __get_date_format(df, dayfirst=True, yearfirst=False, **args):
    """
    This function identifies the format of dates.
    Input: df
    Output: dayfirst and yearfirst arguments that can be
    passed into dateutils.parser
    """
    # Split dates from separator. Result is multiple columns
    # with year, month and day separated
    df = df.astype(str).str.split(r"[-/.]", expand=True)
    # If the split was succesful
    if df.shape[1] > 1:
        # Remove columns that did have problems / did not split
        df = df.dropna().copy()
        # Convert columns to integer if possible
        for col in df.columns:
            if all(df.loc[:, col].astype(str).str.isdigit()):
                df.loc[:, col] = df.loc[:, col].astype(int)
        # Get years, months, and days
        year = list(range(1970, 2050))
        month = list(range(1, 13))
        day = list(range(1, 32))
        # Get those values that distinguish days and years
        year = list(set(year).difference(day))
        day = list(set(day).difference(month))
        # Check if these values can be found from the data
        data = {
            "any_in_day": list(any(df[col].isin(day)) for col in df.columns),
            "any_in_year": list(any(df[col].isin(year)) for col in df.columns)
            }
        df_res = pd.DataFrame(data)
        # Initialize result list, loop over columns that have years,
        # months and days
        result = []
        for i, col in enumerate(df.columns):
            # Get the result of specific column
            temp = df_res.iloc[i, :]
            # Check if it is year
            if temp["any_in_year"]:
                result.append("year")
            # Check if it is day
            elif temp["any_in_day"]:
                result.append("day")
            else:
                result.append("month")
        # If result does not include all year, month and day, use
        # default settings
        if all(i in result for i in ["year", "month", "day"]):
            # If year is first
            yearfirst = True if result.index("year") == 0 else False
            # If day comes before month
            if (yearfirst and
                result.index("day") == 1) or (not yearfirst and
                                              result.index("day") == 2):
                dayfirst = True
            else:
                dayfirst = False
    res = [dayfirst, yearfirst]
    return res
