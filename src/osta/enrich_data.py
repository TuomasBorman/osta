#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import osta.__utils as utils
import pandas as pd
import warnings
import pkg_resources


def enrich_data(df, **args):
    """
    Change column names of pandas.DataFrame

    Arguments:
        ```
        df: pandas.DataFrame containing invoice data.

        ```

    Details:

    Examples:
        ```

        ```

    Output:


    """
    # INPUT CHECK
    # df must be pandas DataFrame
    if not utils.__is_non_empty_df(df):
        raise Exception(
            "'df' must be non-empty pandas.DataFrame."
            )
    # INPUT CHECK END

    # Add organization data
    df = __add_org_data(df)
    return df


def __add_org_data(df, disable_org=False, org_data=None, **args):
    """
    This function prepares organization data to be checked, and calls
    function that checks it.
    Input: df
    Output: df with standardized organization data
    """
    # INPUT CHECK
    if not (utils.__is_non_empty_df(org_data) or org_data is None):
        raise Exception(
            "'org_data' must be non-empty pandas.DataFrame or None."
            )
    if not isinstance(disable_org, bool):
        raise Exception(
            "'disable_org' must be True or False."
            )
    # Check if column(s) is found as non-duplicated
    cols_df = ["org_id", "org_vat_number", "org_number", "org_name"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_df)
    if disable_org or len(cols_to_check) == 0:
        return df
    # INPUT CHECK END
    # Column of db that are matched with columns that are being checked
    # Subset to match with cols_to_check
    cols_to_match = ["bid", "vat_number", "number", "name"]
    cols_to_match = [cols_to_match[i] for i, x in enumerate(cols_df)
                     if x in cols_to_check]
    # Load default database
    if org_data is None:
        # path = pkg_resources.resource_filename(
        #     "osta", "resources/" + "municipality_codes.csv")
        path = "~/Python/osta/src/osta/resources/" + "municipality_codes.csv"
        org_data = pd.read_csv(path, index_col=0)
    # Add data from database
    df = __add_data_from_db(df=df, df_db=org_data,
                            cols_to_check=cols_to_check,
                            cols_to_match=cols_to_match,
                            prefix="org_")
    return df


def __add_data_from_db(df, df_db, cols_to_check, cols_to_match, prefix):
    # Which column are found from df and df_db
    cols_df = [x for x in cols_to_check if x in df.columns]
    cols_df_db = [x for x in cols_to_match if x in df_db.columns]
    # Drop those columns that do not have match in other df
    if len(cols_df) > len(cols_df_db):
        cols_to_check = [cols_df[cols_to_match.index(x)] for x in cols_df_db]
        cols_to_match = cols_df_db
    else:
        cols_to_match = [cols_df_db[cols_to_check.index(x)] for x in cols_df]
        cols_to_check = cols_df
    # Get only the first variable
    col_to_check = cols_to_check[0]
    col_to_match = cols_to_match[0]
    # Get columns that will be added to data/that are not yet included
    cols_to_add = [x for x in df_db.columns if x not in cols_to_match]
    # If there are columns to add
    if len(cols_to_add) > 0:
        # Create temporary columns which are used to merge data
        temp_x = df.loc[:, col_to_check]
        temp_y = df_db.loc[:, col_to_match]
        # Subset database and add prefix tp column names
        df_db = df_db.loc[:, cols_to_add]
        df_db.columns = prefix + df_db.columns
        # If variables can be converted into numeric, do so
        if (all(temp_x.astype(str).str.isnumeric()) and all(
                temp_y.astype(str).str.isnumeric())):
            temp_x = pd.to_numeric(temp_x)
            temp_y = pd.to_numeric(temp_y)
        # Add temporary columns to data
        df.loc[:, "temporary_X"] = temp_x
        df_db.loc[:, "temporary_Y"] = temp_y
        # Merge data
        df = pd.merge(df, df_db, how="left",
                      left_on="temporary_X", right_on="temporary_Y")
        # Remove temproary columns
        df = df.drop(["temporary_X", "temporary_Y"], axis=1)
    return df