#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import re
import numpy as np
import warnings
import codecs
import cchardet
import csv
import filetype


def __is_non_empty_df(df):
    """
    This function checks if value is non empty pd.DataFrame.
    Input: value
    Output: True or False
    """
    res = isinstance(df, pd.DataFrame) and df.shape[0] > 0 and df.shape[1] > 0
    return res


def __is_percentage(value):
    """
    This function checks if value is numeric and [0,1].
    Input: value
    Output: True or False
    """
    res = ((isinstance(value, int) or isinstance(value, float)) and
           (0 <= value <= 1))
    res2 = not isinstance(value, bool)
    res = res and res2
    return res


def __are_valid_bids(values):
    """
    This function checks if values are valid business IDs.
    Input: pd.Series
    Output: pd.Series of boolean values
    """
    # Get original indices that are used to preserve the order
    ind = values.index
    # If values contain correct pattern
    patt = "^\\d\\d\\d\\d\\d\\d\\d-\\d$"
    contains_patt = values.astype(str).str.contains(patt)
    # Get those values that include the pattern
    values = values[contains_patt]
    # Initialize the result: which values do not include the pattern?
    res = contains_patt[-contains_patt]
    # If there were values that include the pattern
    if len(values) > 0:
        # Split values from "-" to extract BID's first part and its check mark
        values = values.astype(str).str.split("-", expand=True)
        # Get check marks
        check_marks = values.iloc[:, 1]
        # Divide first part's numbers to own columns
        values = values.iloc[:, 0].apply(lambda x: pd.Series(list(x)))
        # Convert each column to integer
        values = values.apply(lambda x: x.astype(int))
        # To each column, different weight is applied
        weights = [7, 9, 10, 5, 8, 4, 2]
        values = values*weights
        # Each row (bid) is summed-up
        values = values.sum(axis=1)
        # The sums are divided by 11, --> get the moduluses
        values = values % 11
        # The result is (11 - modulus) unless the modulus is 0.
        # Then the result is 0.
        values = 11 - values
        values[values == 11] = 0
        # Test if the results match with their corresponding check mark
        values = values == check_marks.astype(int)
        # Add the result of check mark calculation to results
        res = pd.concat([res, values])
    # Preserve the order
    res = res[ind]
    return res


def __are_valid_vat_numbers(values):
    """
    This function checks if values are valid VAT numbers.
    Input: pd.Series
    Output: pd.Series of boolean values
    """
    # VAT number includes specific pattern of characters along
    # with land code. Test if it is found
    patt_to_search = [
        # Finland
        "^FI\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Belgium
        "^BE\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Bulgaria
        "^BG\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        "^BG\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Spain
        "^ES[A-Z0-9]\\d\\d\\d\\d\\d\\d\\d[A-Z0-9]$",
        # Netherlands
        "^NL\\d\\d\\d\\d\\d\\d\\d\\d\\d[B]\\d\\d$",
        # Ireland
        "^IE\\d[A-Z0-9_.-/\\+=(){}?!]\\d\\d\\d\\d\\d[A-Z]$",
        "^IE\\d[A-Z0-9_.-/\\+=(){}?!]\\d\\d\\d\\d\\d[A-Z][A-Z]$",
        # Great Britain
        "^GB\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        "^GB\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        "^GBGD\\d\\d\\d$",
        "^GBHA\\d\\d\\d$",
        # Northern Ireland
        "^XI\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        "^XI\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        "^XIGD\\d\\d\\d$",
        "^XIHA\\d\\d\\d$",
        # Italy
        "^IT\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Austria
        "^ATU\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Greece
        "^EL\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Croatia
        "^HR\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Cypros
        "^CY\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d[A-Z]$",
        # Latvia
        "^LV\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Lithuenia
        "^LT\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        "^LT\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Luxemburg
        "^LU\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Malta
        "^MT\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Portugal
        "^PT\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Poland
        "^PL\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # France
        "^FR[A-Z0-9][A-Z0-9]\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Romania
        "^RO[0-9]{2,10}$",
        # Sweden
        "^SE\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d01$",
        # Germany
        "^DE\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Slovakia
        "^SK\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Slovenia
        "^SI\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Denmark
        "^DK\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Czech Republic
        "^CZ[0-9]{9,10}$"
        # Hungary
        "^HU\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        # Estonia
        "^EE\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d$",
        ]
    # Remove spaces if there are any; find patterns with case
    # insensitive search
    res = values.astype(str).str.replace(" ", "").str.contains(
        "|".join(patt_to_search), flags=re.IGNORECASE)
    # Combine results
    return res


def __test_if_date(df, wo_sep=False, **args):
    """
    This function checks if the column defines dates
    Input: Series
    Output: Boolean value
    """
    # Initialize result
    res = False
    df = df.dropna()
    if len(df) > 0 and df.dtype == "datetime64":
        res = True
    elif len(df) > 0 and df.dtype in ["object"]:
        patt_to_search = [
            "^\\d\\d[-/.]\\d\\d[-/.]\\d\\d\\d\\d$",
            "^\\d[-/.]\\d\\d[-/.]\\d\\d\\d\\d$",
            "^\\d\\d[-/.]\\d[-/.]\\d\\d\\d\\d$",
            "^\\d[-/.]\\d[-/.]\\d\\d\\d\\d$",
            "^\\d[-/.]\\d[-/.]\\d\\d$",
            "^\\d[-/.]\\d[-/.]\\d\\d\\d\\d$",

            "^\\d\\d\\d\\d[-/.]\\d\\d[-/.]\\d\\d$",
            "^\\d\\d\\d\\d[-/.]\\d[-/.]\\d\\d$",
            "^\\d\\d\\d\\d[-/.]\\d\\d[-/.]\\d$",
            "^\\d\\d[-/.]\\d[-/.]\\d$",
            ]
        patt_wo_sep = [
            "^\\d\\d\\d\\d\\d\\d\\d\\d$",
            "^\\d\\d\\d\\d\\d\\d\\d$",
            "^\\d\\d\\d\\d\\d\\d\\d$",
            "^\\d\\d\\d\\d\\d\\d$",
            "^\\d\\d\\d\\d\\d$",
            "^\\d\\d\\d\\d$"
            ]
        # If specified search also dates without separators
        if wo_sep:
            patt_to_search.extend(patt_wo_sep)
        # Find patterns
        patt_found = df.astype(str).str.contains("|".join(patt_to_search))
        if all(patt_found):
            res = True
    return res


def __test_if_voucher(df, col_i, colnames):
    """
    This function checks if the column defines vouchers
    Input: DataFrame, index of the column, found final column names
    Output: Boolean value
    """
    # Initialize result
    res = False
    # If data includes already dates and values of column are increasing
    # and they are not dates,the column includes voucher values
    if "date" in colnames and df.iloc[:, col_i].is_monotonic_increasing and \
            not df.iloc[:, col_i].equals(df.iloc[:, colnames.index("date")]):
        res = True
    else:
        test_res = []
        # List variables that are matched/checked
        variable_list = [
            ["org_number", "org_id", "org_name"],  # Organization
            ["suppl_id", "suppl_name"],  # Supplier
            ["account_name", "account_number"],  # Account
            ["service_cat", "service_cat_number"],  # Service category
            ["date"],   # Date
            ]
        # List thresholds that are used
        thresholds = [
            100,  # Organization
            2,  # Supplier
            5,  # Account
            5,  # Service category
            1.5,  # Date
            ]
        # If variables were found from the colnames
        for i, variables in enumerate(variable_list):
            # Test if column match with prerequisites of voucher column
            temp_res = __test_if_voucher_help(df=df,
                                              col_i=col_i,
                                              colnames=colnames,
                                              variables=variables,
                                              voucher_th=thresholds[i],
                                              )
            test_res.append(temp_res)
        # If not float, then  it is not sum
        if df.dtypes[col_i] == "float64":
            test_res.append(False)
        else:
            test_res.append(True)
        # If all test were True, the result is True
        if all(test_res):
            res = True
    return res


def __test_if_voucher_help(df, col_i, colnames, variables, voucher_th):
    """
    This function is a help function for voucher tester.
    This function tests if there are more unique values than there are
    tested values
    Input: DataFrame, index of the column, found final column names
    Output: Boolean value
    """
    # Initialize results
    res = False
    # Check which variables are shared between variables and colnames
    var_shared = [x for x in colnames if x in variables]
    # If variables were found from the colnames
    if len(var_shared) > 0:
        # Get only specified columns
        temp = df.iloc[:, [colnames.index(var) for var in var_shared]]
        # Remove rows with NA
        temp = temp.dropna()
        # Drop duplicates, now we have unique rows
        temp = temp.drop_duplicates()
        # Add column to variables
        var_shared.append(np.unique(df.columns[col_i])[0])
        # Get only specified columns with column that is being checked
        temp_col = df.iloc[:, [colnames.index(var) for var in var_shared]]
        # Remove rows with NA
        temp_col = temp_col.dropna()
        # Drop duplicates, now we have unique rows
        temp_col = temp_col.drop_duplicates()
        # If there are voucher_th times more unique rows, the column
        # is not related to columns that are matched
        if temp_col.shape[0] > temp.shape[0]*voucher_th:
            res = True
    return res


def __not_duplicated_columns_found(df, cols_to_check):
    """
    This function checks if specific columns can be found and they are
    duplicated.
    Input: df, columns, duplicated columns
    Output: columns that fulfill criteria
    """
    # Found columns
    cols_to_check = [x for x in cols_to_check if x in df.columns]
    # Get columns from df
    cols_df = [x for x in df.columns if x in cols_to_check]
    # Get unique values and their counts
    unique, counts = np.unique(cols_df, return_counts=True)
    # Get duplicated values
    duplicated = unique[counts > 1]
    # Are columns found and not duplicated? Return True if any found.
    cols_to_check = [x for x in cols_to_check if x not in duplicated]
    # If there were duplicated columns, give warning
    if len(duplicated) > 0:
        warnings.warn(
            message=f"The following column names are duplicated. "
            f"Please check them for errors.\n {duplicated.tolist()}",
            category=Warning
            )
    return cols_to_check


def __subset_data_based_on_year(df, df_db, db_year=None,
                                date_format="%d-%m-%Y", **args):
    """
    This function subsets database by taking only specific years that user
    has specified or that can be found from the data.
    Input: df, data base, year_option, date_format
    Output: Subsetted data base
    """
    # INPUT CHECK
    if not (db_year is None or isinstance(db_year, str) or
            isinstance(db_year, int)):
        raise Exception(
            "'db_year' must be None, string or integer specifying a year."
            )
    # INPUT CHECK END
    if db_year is not None:
        db_year = [db_year] if isinstance(db_year, int) else db_year
        db_year = [x for x in db_year if any(x == df_db["year"])]
        if len(db_year) > 0:
            # Subset data
            ind = (df_db["year"] <= max(db_year)).values & (
                df_db["year"] >= min(db_year)).values
            df_db = df_db.loc[ind, :]
        else:
            years = df_db["year"].drop_duplicates().values.tolist()
            raise Exception(
                f"'db_year' must be one of the following options: "
                f"{years}",
                )
    else:
        # Check if column(s) is found as non-duplicated
        cols_to_check = ["date"]
        cols_to_check = __not_duplicated_columns_found(df, cols_to_check)
        if len(cols_to_check) == 1:
            col_to_check = cols_to_check[0]
            # INPUT CHECK END
            # Get date column
            date = df.loc[:, col_to_check]
            # Extract year if possible
            if "date" in df.columns:
                try:
                    year = pd.to_datetime(date, format=date_format)
                    year = date.dt.year.drop_duplicates().sort_values()
                    df_db = df_db.loc[df_db["year"] == year, :]
                except Exception:
                    pass
    # Get only unique values
    df_db = df_db.drop_duplicates(subset=["number", "name"])
    return df_db


def __detect_format_and_open_file(
        file_path,
        encoding=None, guess_encoding=True,
        delimiter=None, guess_delimiter=True,
        **args):
    """
    This function is a helper function to load and detect the format of the
    file that cab be found from the disk and that is not zipped.
    The returned value is pandas.DataFrame.
    """
    # INPUT CHECK
    if not (isinstance(encoding, str) or encoding is None):
        raise Exception(
            "'encoding' must be a string or None."
            )
    if not isinstance(guess_encoding, bool):
        raise Exception(
            "'guess_encoding' must be a boolean value."
            )
    if not (isinstance(delimiter, str) or delimiter is None):
        raise Exception(
            "'delimiter' must be a string or None."
            )
    if not isinstance(guess_delimiter, bool):
        raise Exception(
            "'guess_delimiter' must be a boolean value."
            )
    # INPUT CHECK END
    # Check if the file is excel file
    file_type = filetype.guess(file_path)
    if file_type is not None and (file_type.extension == "xls" or
                                  file_type.extension == "xlsx"):
        file_type = "excel"
    elif file_type is None or file_type.extension == "csv":
        # Otherwise try filetype csv unless filetype is guessed to be other
        file_type = "csv"
    else:
        raise Exception(
            "The format of the file is not supported."
            )

    # If encoding is not determined and user wants to guess it for csv file
    if file_type == "csv" and encoding is None and guess_encoding:
        # Open the data
        rawdata = open(file_path, "rb")
        # Check its encoding
        encoding = cchardet.detect(rawdata.read())["encoding"]

    # If delimiter is not specified and user wants to guess it for csv file
    if file_type == "csv" and delimiter is None and guess_delimiter:
        # Open the data
        rawdata = codecs.open(file_path, 'r', encoding=encoding)
        # Get delimiter
        delimiter = csv.Sniffer().sniff(rawdata.read()).delimiter
        # If the delimiter is just space, the file contains empty strings for
        # the missing entries --> empty space is not correct delimiter, try \t
        if delimiter == " ":
            delimiter = "\t"

    # Based on file format, open the file with correct function
    if file_type == "excel":
        df = pd.read_excel(file_path, **args, dtype='unicode')
    else:
        df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter,
                         **args, dtype='unicode')
    return df
