#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pkg_resources

# Fetch DataFrame that contains fields and create a dictionary from it.
# Input: A dictionary of fields or nothing.
# Output: A dictionary containing which column names are meaning the same


def _get_fields_df(fields):
    # If fields was not provided, open files that include fields
    if fields is None:
        # Load data from /resources
        path = pkg_resources.resource_filename('ostan',
                                               "resources/" +
                                               "mandatory_fields.csv")
        mandatory_fields = pd.read_csv(path
                                       ).set_index("key")["value"].to_dict()
        path = pkg_resources.resource_filename('ostan',
                                               "resources/" +
                                               "optional_fields.csv")
        optional_fields = pd.read_csv(path
                                      ).set_index("key")["value"].to_dict()
        # Combine fields into one dictionary
        fields = {}
        fields.update(mandatory_fields)
        fields.update(optional_fields)
        # Add field values as a key
        add_fields = pd.DataFrame(fields.values(), fields.values())
        add_fields = add_fields[0].to_dict()
        fields.update(add_fields)
    return fields


def _guess_name(df, col, colnames, fields,
                match_th=0.9, scorer=fuzz.token_sort_ratio, **args):
    # INPUT CHECK
    # Types of all other arguments are fixed
    # match_th must be numeric value 0-1
    if not ((isinstance(match_th, int) or isinstance(match_th, float)) and
            (0 <= match_th <= 1)):
        raise Exception(
            "'match_th' must be a number between 0-1."
            )
    # INPUT CHECK END

    # Try if column is ID column
    if _test_if_BID(df=df, col=col, **args):
        # BID can be from organization or supplier
        col = _org_or_suppl_BID(df=df, col=col, colnames=colnames)
    # Test if date
    elif _test_if_date(df=df, col=col, colnames=colnames):
        col = "date"
    # Test if column includes country codes
    elif _test_if_country(df, col, colnames, **args):
        col = "country"
    # Test if org_number
    elif _test_match_between_colnames(df=df, col=col, colnames=colnames,
                                      cols_match=["org_name", "org_id"],
                                      datatype=["int64"]
                                      ):
        col = "org_number"
    # Test if org_name
    elif _test_match_between_colnames(df=df, col=col, colnames=colnames,
                                      cols_match=["org_number", "org_id"],
                                      datatype=["object"]
                                      ):
        col = "org_name"
    # Test if suppl_name
    elif _test_match_between_colnames(df=df, col=col, colnames=colnames,
                                      cols_match=["suppl_id"],
                                      datatype=["object"]
                                      ):
        col = "suppl_name"
    # Test if service_cat
    elif _test_match_between_colnames(df=df, col=col, colnames=colnames,
                                      cols_match=["service_cat_name"],
                                      datatype=["object", "int64"]
                                      ):
        col = "service_cat"
    # Test if service_cat_name
    elif _test_match_between_colnames(df=df, col=col, colnames=colnames,
                                      cols_match=["service_cat"],
                                      datatype=["object"]
                                      ):
        col = "service_cat_name"
    # Test if account_number
    elif _test_match_between_colnames(df=df, col=col, colnames=colnames,
                                      cols_match=["account_name"],
                                      datatype=["int64"]
                                      ):
        col = "account_number"
    # Test if account_name
    elif _test_match_between_colnames(df=df, col=col, colnames=colnames,
                                      cols_match=["account_number"],
                                      datatype=["object"]
                                      ):
        col = "account_name"
    # test if price_ex_vat
    elif _test_if_sums(df=df, col=col, colnames=colnames,
                       test_sum="price_ex_vat",
                       match_with=["total", "vat_amount"],
                       datatype="float64"
                       ):
        col = "price_ex_vat"
    # test if total
    elif _test_if_sums(df=df, col=col, colnames=colnames,
                       test_sum="total",
                       match_with=["vat_amount", "price_ex_vat"],
                       datatype="float64"
                       ):
        col = "total"
    # test if vat_amount
    elif _test_if_sums(df=df, col=col, colnames=colnames,
                       test_sum="vat_amount",
                       match_with=["total", "price_ex_vat"],
                       datatype="float64"
                       ):
        col = "vat_amount"
    # Test if voucher
    elif _test_if_voucher(df=df, col=col, colnames=colnames):
        col = "voucher"
    elif col.strip():
        # Try partial match if column name is not empty
        # Get the most similar key value
        col_name_part = process.extractOne(col, fields.keys(),
                                           scorer=scorer)
        # If the matching score is over threshold
        match_th = match_th*100  # float value to a number between 0-100
        if col_name_part[1] >= match_th:
            # Get only the key name
            col_name_part = col_name_part[0]
            # Based on the key, get the value
            col = fields.get(col_name_part)
    return col


# This function checks if the column defines BIDs (y-tunnus)
# Input: DataFrame, name of the column, found final column names
# Output: Boolean value


def _test_if_BID(df, col, bid_patt_th=0.8, **args):
    # INPUT CHECK
    # bid_patt_th must be numeric value 0-100
    if not ((isinstance(bid_patt_th, int) or
             isinstance(bid_patt_th, float)) and
            (0 <= bid_patt_th <= 1)):
        raise Exception(
            "'bid_patt_th' must be a number between 0-1."
            )
    # INPUT CHECK END

    # Initialize result as False
    res = False
    # Test if pattern found
    patt_found = df.loc[:, col].astype(str).str.contains(
        "\\d\\d\\d\\d\\d\\d\\d-\\d")
    patt_found = patt_found.value_counts()/df.shape[0]
    # Test of length correct
    len_correct = df.loc[:, col].astype(str).str.len() == 9
    len_correct = len_correct.value_counts()/df.shape[0]
    # If Trues exist in both, get the smaller portion. Otherwise, True was not
    # found and the result is 0 / not found
    if True in patt_found.index and True in len_correct.index:
        # Get portion of Trues and take only value
        patt_found = patt_found[patt_found.index][0]
        len_correct = len_correct[len_correct.index][0]
        # Get smaller value
        patt_found = min(patt_found, len_correct)
    else:
        patt_found = 0
    # Check if over threshold
    if patt_found > bid_patt_th:
        res = True
    return res


# This function checks if the column defines BID of organization or supplier
# Input: DataFrame, name of the column, found final column names
# Output: The final colname of BID column


def _org_or_suppl_BID(df, col, colnames):
    # Initialize result as supplier ID
    res = "suppl_id"
    # List of columns that are matched
    cols_match = ["org_number", "org_name"]
    # Loop over columns that should be matched
    for col_match in cols_match:
        # If the column is in colnames
        if col_match in colnames:
            # Subset the data by taking only specified columns
            temp = df.iloc[:, [colnames.index(col),
                               colnames.index(col_match)]]
            # Drop rows with blank values
            temp = temp.dropna()
            # Number of unique combinations
            n_uniq = temp.drop_duplicates().shape[0]
            # If there are as many combinations as there are individual values
            # these columns match
            if n_uniq == df.iloc[:, colnames.index(col_match)].nunique():
                res = "org_id"
    # If there are supplier IDs already, try if they are differemt
    if "suppl_id" in colnames and all(df.iloc[:, colnames.index(col)] !=
                                      df.iloc[:, colnames.index("suppl_id")]):
        res = "org_id"
    # If there are organization IDs already, try if they are differemt
    if "org_id" in colnames and all(df.iloc[:, colnames.index(col)] ==
                                    df.iloc[:, colnames.index("org_id")]):
        res = "org_id"
    # If there are not many unique values, it might be organization ID
    if df.iloc[:, colnames.index(col)].nunique()/df.shape[0] < 0.5:
        res = "org_id"
    return res


# This function checks if the column defines dates
# Input: DataFrame, name of the column, found final column names
# Output: Boolean value


def _test_if_date(df, col, colnames):
    # Initialize result
    res = False
    df = df.iloc[:, colnames.index(col)]
    df = df.dropna()
    if df.dtype == "datetime64":
        res = True
    elif df.dtype in ["int64", "object"]:
        patt_to_search = [
            "\\d\\d\\d\\d\\d\\d\\d\\d",
            "\\d\\d\\d\\d\\d\\d\\d",
            "\\d\\d\\d\\d",

            "\\d\\d[.-/]\\d\\d[.-/]\\d\\d\\d\\d",
            "\\d[.-/]\\d\\d[.-/]\\d\\d\\d\\d",
            "\\d\\d[.-/]\\d[.-/]\\d\\d\\d\\d",
            "\\d[.-/]\\d[.-/]\\d\\d",

            "\\d\\d\\d\\d[.-/]\\d\\d[.-/]\\d\\d",
            "\\d\\d\\d\\d[.-/]\\d[.-/]\\d\\d",
            "\\d\\d\\d\\d[.-/]\\d\\d[.-/]\\d",
            "\\d\\d\\d[.-/]\\d[.-/]",
            ]
        patt_found = df.astype(str).str.contains("|".join(patt_to_search))
        if all(patt_found):
            res = True
    return res


# This function checks if the column defines extra information of
# another column / if the column is related to that
# Input: DataFrame, name of the column, found final column names
# Output: Boolean value


def _test_match_between_colnames(df, col, colnames, cols_match, datatype):
    # Initialize results as False
    res = False
    # Test the data type
    if df.dtypes[colnames.index(col)] in datatype:
        # Loop over columns that should be matched
        for col_match in cols_match:
            # If the column is in colnames
            if col_match in colnames:
                # Subset the data by taking only specified columns
                temp = df.iloc[:, [colnames.index(col),
                                   colnames.index(col_match)]]
                # Drop rows with blank values
                temp = temp.dropna()
                # Number of unique combinations
                n_uniq = temp.drop_duplicates().shape[0]
                # If there are as many combinations as there are
                # individual values these columns match
                if n_uniq == df.iloc[:, colnames.index(col_match)].nunique():
                    res = True
    return res


# This function checks if the column defines total, net, or VAT sum,
# the arguments defines what is searched
# Input: DataFrame, name of the column, found final column names
# Output: Boolean value


def _test_if_sums(df, col, colnames, test_sum, match_with, datatype):
    # Initialize results as False
    res = False
    # If all columns are available
    if all(mw in colnames for mw in match_with):
        # Take only specific columns
        ind = list(colnames.index(mw) for mw in match_with)
        ind.append(colnames.index(col))
        df_temp = df.iloc[:, ind]
        # Drop empty rows
        df_temp = df_temp.dropna()

        # If the datatypes are correct
        if all(df_temp.dtypes == datatype):
            # If VAT is tested and value is correct
            if test_sum == "vat_amount" and\
                all(df_temp.iloc[:, colnames.index(col)] ==
                    df_temp.iloc[:, colnames.index("total")] -
                    df_temp.iloc[:, colnames.index("price_ex_vat")]):
                res = True
            # If total is tested and value is correct
            elif test_sum == "total" and\
                all(df_temp.iloc[:, colnames.index(col)] ==
                    df_temp.iloc[:, colnames.index("price_ex_vat")] +
                    df_temp.iloc[:, colnames.index("vat_amount")]):
                res = True
            # If price_ex_vat is tested and value is correct
            elif test_sum == "price_ex_vat" and\
                all(df_temp.iloc[:, colnames.index(col)] ==
                    df_temp.iloc[:, colnames.index("total")] -
                    df_temp.iloc[:, colnames.index("vat_amount")]):
                res = True
    return res


# This function checks if the column defines countries
# Input: DataFrame, name of the column, found final column names
# Output: Boolean value


def _test_if_country(df, col, colnames, country_code_th=0.2, **args):
    # INPUT CHECK
    # country_code_th must be numeric value 0-100
    if not ((isinstance(country_code_th, int) or
             isinstance(country_code_th, float)) and
            (0 <= country_code_th <= 1)):
        raise Exception(
            "'country_code_th' must be a number between 0-1."
            )
    # INPUT CHECK END

    # Initialize results as False
    res = False
    # Get specific column and remove NaNs
    df = df.iloc[:, colnames.index(col)]
    df = df.dropna()
    # Test if col values can be found from the table
    path = pkg_resources.resource_filename('ostan',
                                           "resources/" +
                                           "land_codes.csv")
    codes = pd.read_csv(path, index_col=0)
    # Drop numeric codes, since we cannot be sure that they are land codes
    codes = codes.drop("Numeerinen koodi [2]", axis=1)
    res_df = pd.DataFrame()
    for name, data in codes.items():
        res_df[name] = (df.isin(data))
    # How many times the value was found from the codes? If enough, then we
    # can be sure that the column includes land codes
    if sum(res_df.sum(axis=1) > 0)/res_df.shape[0] > country_code_th:
        res = True
    return res


# This function checks if the column defines vouchers
# Input: DataFrame, name of the column, found final column names
# Output: Boolean value

def _test_if_voucher(df, col, colnames):
    # Initialize result
    res = False
    # If data includes already dates and values of column are increasing
    # and they are not dates, the column includes voucher values
    if "date" in colnames and df.loc[:, col].is_monotonic_increasing and \
            not df.loc[:, col].equals(df.iloc[:, colnames.index("date")]):
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
            temp_res = _test_if_voucher_help(df=df,
                                             col=col,
                                             colnames=colnames,
                                             variables=variables,
                                             voucher_th=thresholds[i],
                                             )
            test_res.append(temp_res)
        # If not float, then  it is not sum
        if df.dtypes[colnames.index(col)] == "float64":
            test_res.append(False)
        else:
            test_res.append(True)
        # If all test were True, the result is True
        if all(test_res):
            res = True
    return res


# This function is a help function for voucher tester.
# This function tests if there are more unique values than there are
# tested values
# Input: DataFrame, name of the column, found final column names
# Output: Boolean value


def _test_if_voucher_help(df, col, colnames, variables, voucher_th):
    # Initialize results
    res = False
    # CHeck which variables are shared between variables and colnames
    var_shared = list(set(colnames) & set(variables))
    # If variables were found from the colnames
    if len(var_shared) > 0:
        # Get only specified columns
        temp = df.iloc[:, [colnames.index(var) for var in var_shared]]
        # Remove rows with NA
        temp = temp.dropna()
        # Drop duplicates, now we have unique rows
        temp = temp.drop_duplicates()
        # Add column to variables
        var_shared.append(col)
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