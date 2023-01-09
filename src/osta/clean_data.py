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
    col_found = [x for x in col_to_check if x in df.columns]
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


def __standardize_date(df, date_format="%d-%m-%Y",
                       dayfirst=True, yearfirst=False, **args):
    """
    This function identifies the format of dates and standardize them.
    Input: df
    Output: df
    """
    # Check if column can be found and it is not duplicated
    # If not, keep df unchanged
    if not __col_present_and_not_duplicated("date", df.columns):
        return df

    # Get date column
    df_date = df.loc[:, "date"]
    # Split dates from separator. Result is multiple columns
    # with year, month and day separated
    df_date = df_date.astype(str).str.split(r"[-/.]", expand=True)
    # If the split was succesful
    if df_date.shape[1] > 1:
        # Remove columns that did have problems / did not split
        df_date = df_date.dropna().copy()
        # Convert columns to numeric if possible
        for c in df_date.columns:
            if all(df_date.loc[:, c].astype(str).str.isnumeric()):
                df_date[c] = pd.to_numeric(df_date[c])
        # Get years, months, and days
        year = list(range(1970, 2050))
        month = list(range(1, 13))
        day = list(range(1, 32))
        # Get those values that distinguish days and years
        year = list(set(year).difference(day))
        day = list(set(day).difference(month))
        # Check if these values can be found from the data
        data = {
            "any_in_day": list(any(df_date[c].isin(day))
                               for c in df_date.columns),
            "any_in_year": list(any(df_date[c].isin(year))
                                for c in df_date.columns)
            }
        df_res = pd.DataFrame(data)
        # Initialize result list, loop over columns that have years,
        # months and days
        result = []
        for i, c in enumerate(df_date.columns):
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
                                              result.index("day") == 0):
                dayfirst = True
            else:
                dayfirst = False
        # Standardize dates
        df_date = pd.to_datetime(df.loc[:, "date"],
                                 dayfirst=dayfirst, yearfirst=yearfirst)
        # Change the formatting
        df_date = df_date.dt.strftime(date_format)
        # Assign values back to data frame
        df.loc[:, "date"] = df_date
    # If the format cannot be detected, disabel date standardization
    else:
        warnings.warn(
            message="The format of dates where not detected, "
            "and the 'date' column is unchanged. Please check that dates "
            "have separators between days, months, and years.",
            category=Warning
            )
    return df


def __standardize_org(df, org_data=None, **args):
    """
    This function prepares organization data to be checked, and calls
    function that checks it.
    Input: df
    Output: df with standardized organization data
    """
    # INPUT CHECK
    if not isinstance(org_data, pd.DataFrame) or\
        org_data.shape[0] == 0 or\
            org_data.shape[1] == 0:
        raise Exception(
            "'org_data' must be non-empty pandas.DataFrame or None."
            )
    # INPUT CHECK END
    if org_data is None:
        print("load organization data")
    # Standardize data
    # Column that are checked from df
    cols_to_check = ["org_number", "org_name", "org_id"]
    # Column of db that are matched with columns that are being checked
    cols_to_match = ["code", "name", "bid"]
    # Standardize organization data
    df = __standardize_org_or_suppl(df=df, df_db=org_data, name_db="org_data",
                                    cols_to_check=cols_to_check,
                                    cols_to_match=cols_to_match,
                                    **args)
    return df


def __standardize_org_or_suppl(df, df_db, name_db,
                               cols_to_check, cols_to_match,
                               match_th=0.7, **args):
    """
    Standardize the data based on database.
    Input: df, df_db including database,
    match_th to be used to match partial matching names
    Output: Standardized data
    """
    # INPUT CHECK
    # match_th must be numeric value 0-1
    if not (((isinstance(match_th, int) or isinstance(match_th, float)) and
             not isinstance(match_th, bool)) and
            (0 <= match_th <= 1)):
        raise Exception(
            "'match_th' must be a number between 0-1."
            )
    # INPUT CHECK END
    # Which column are found from df and df_db
    cols_to_check = [x for x in cols_to_check if x in df.columns]
    cols_to_match = [x for x in cols_to_match if x in df_db.columns]
    # If none was found from the data base
    if len(cols_to_check) > 0 or len(cols_to_match) == 0:
        warnings.warn(
            message=f"'{name_db}' should include at least one of the "
            "following columns: 'name' (name), 'number' "
            "(number), and 'bid' (business ID).",
            category=Warning
            )
        return df
    # If data includes organization data columns
    elif len(cols_to_check) == 0:
        return df
    return df
def __col_present_and_not_duplicated(col, colnames):
    """
    This function checks if column is present. Also it check if there
    are duplicated column and gives corresponding warning
    Input: column being checked and column names
    Output: True or False
    """
    # Check if column is present
    if col in colnames:
        res = True
    else:
        res = False
        warnings.warn(
            message=f"The following column cannot be found from the data "
            f": {col}",
            category=Warning
            )
    # Check if it is duplicated
    if sum(list(c == col for c in colnames)) > 1:
        res = False
        warnings.warn(
            message=f"The following column is duplicated, and values will be "
            f"unchanged. Please check it for errors: {col}",
            category=Warning
            )
    return res
