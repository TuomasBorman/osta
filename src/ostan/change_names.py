# -*- coding: utf-8 -*-
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import warnings


def change_names(df, fields=None, guess_names=True, **args):
    # INPUT CHECK
    # df must be pandas DataFrame
    if not isinstance(df, pd.DataFrame):
        raise Exception(
            "'df' must be pandas.DataFrame."
            )
    # fields must be pandas DataFrame or None
    if not (isinstance(fields, dict) or fields is None):
        raise Exception(
            "'fields' must be dict or None."
            )
    # guess_names must be boolean
    if not isinstance(guess_names, bool):
        raise Exception(
            "'guess_names' must be bool."
            )
    # INPUT CHECK END

    # If fields is None, get them
    if fields is None:
        fields = get_fields_df()
    # Initialize lists for column names
    colnames = []
    colnames_not_found = []
    # Loop over column names
    for col in df.columns:
        # Column name to lower case and remove spaces from beginning and end
        # Get matching value from the dictionary
        col_name = fields.get(col.lower().strip())
        # If exact match was not found
        if col_name is None:
            # Do not change the colum name,
            # and add it to not-found column names list
            col_name = col
            colnames_not_found.append(col_name)
        # Append the list of column names
        colnames.append(col_name)

    # If there are column names that were not detected and user wants them
    # to be guessed
    if len(colnames_not_found) > 0 and guess_names:
        # Initialize list for new and old column names for warning message
        colnames_old = []
        colnames_new = []
        for col in colnames_not_found:
            name = guess_name(df=df, col=col, colnames=colnames,
                              fields=fields, **args)
            # if the column name was changed
            if col != name:
                # Change name
                colnames[colnames.index(col)] = name
                # Remove from list
                colnames_not_found.remove(col)
                # Append old and new column name list
                colnames_old.append(col)
                colnames_new.append(name)
        # If there are columns that were changed, give warning
        warnings.warn(
            message=f"The following column names... \n {colnames_old}\n"
            f"... were replaced with \n {colnames_new}",
            category=Warning
            )
    # Replace column names with new ones
    df.columns = colnames

    # Give warning if there were column names that were not identified
    if colnames_not_found:
        warnings.warn(
            message=f"The following column names were not detected. "
            f"Please check them for errors.\n {colnames_not_found}",
            category=Warning
            )
    return df


# Fetch DataFrame that contains fields and create a dictionary from it.
# Input: -
# Output: A dictionary containing which column names are meaning the same


def get_fields_df():
    # Open files that include fields
    # CHANGE THE PATHS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    mandatory_fields = pd.read_csv("~/Python/ostan/data/mandatory_fields.csv"
                                   ).set_index("key")["value"].to_dict()
    optional_fields = pd.read_csv("~/Python/ostan/data/optional_fields.csv"
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


def guess_name(df, col, colnames, fields,
               match_th=0.85, scorer=fuzz.token_sort_ratio, **args):
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
    if test_if_BID(df=df, col=col, **args):
        # BID can be from organization or supplier
        col = org_or_suppl_BID(df=df, col=col, colnames=colnames)
    # Test if date
    elif test_if_date(df=df, col=col, colnames=colnames):
        col = "date"
    # Test if column includes country codes
    elif test_if_country(df, col, colnames, **args):
        col = "country"
    # Test if org_number
    elif test_match_between_colnames(df=df, col=col, colnames=colnames,
                                     cols_match=["org_name", "org_id"],
                                     datatype=["int64"]
                                     ):
        col = "org_number"
    # Test if org_name
    elif test_match_between_colnames(df=df, col=col, colnames=colnames,
                                     cols_match=["org_number", "org_id"],
                                     datatype=["object"]
                                     ):
        col = "org_name"
    # Test if suppl_name
    elif test_match_between_colnames(df=df, col=col, colnames=colnames,
                                     cols_match=["suppl_id"],
                                     datatype=["object"]
                                     ):
        col = "suppl_name"
    # Test if service_cat
    elif test_match_between_colnames(df=df, col=col, colnames=colnames,
                                     cols_match=["service_cat_name"],
                                     datatype=["object", "int64"]
                                     ):
        col = "service_cat"
    # Test if service_cat_name
    elif test_match_between_colnames(df=df, col=col, colnames=colnames,
                                     cols_match=["service_cat"],
                                     datatype=["object"]
                                     ):
        col = "service_cat_name"
    # Test if account_number
    elif test_match_between_colnames(df=df, col=col, colnames=colnames,
                                     cols_match=["account_name"],
                                     datatype=["int64"]
                                     ):
        col = "account_number"
    # Test if account_name
    elif test_match_between_colnames(df=df, col=col, colnames=colnames,
                                     cols_match=["account_number"],
                                     datatype=["object"]
                                     ):
        col = "account_name"
    # test if price_ex_vat
    elif test_if_sums(df=df, col=col, colnames=colnames,
                      greater_cols=["total"],
                      less_cols=["vat_amount"],
                      datatype="float64"
                      ):
        col = "price_ex_vat"
    # test if total
    elif test_if_sums(df=df, col=col, colnames=colnames,
                      greater_cols=[],
                      less_cols=["vat_amount", "price_ex_vat"],
                      datatype="float64"
                      ):
        col = "total"
    # test if vat_AMOUNT
    elif test_if_sums(df=df, col=col, colnames=colnames,
                      greater_cols=["total", "price_ex_vat"],
                      less_cols=[],
                      datatype="float64"
                      ):
        col = "vat_amount"
    # Test if voucher
    elif df.iloc[:, colnames.index(col)].nunique() == df.shape[0]:
        col = "voucher"
    elif not col.strip():
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


def test_if_BID(df, col, patt_found_th=0.8, char_len_th=0.8, **args):
    # INPUT CHECK
    # patt_found_th must be numeric value 0-100
    if not ((isinstance(patt_found_th, int) or
             isinstance(patt_found_th, float)) and
            (0 <= patt_found_th <= 1)):
        raise Exception(
            "'patt_found_th' must be a number between 0-1."
            )
    # char_len_th must be numeric value 0-100
    if not ((isinstance(char_len_th, int) or
             isinstance(char_len_th, float)) and
            (0 <= char_len_th <= 1)):
        raise Exception(
            "'char_len_th' must be a number between 0-1."
            )
    # INPUT CHECK END

    # Initialize result as False
    res = False
    # Test if pattern found
    patt_found = df.loc[:, col].astype(str).str.contains(
        "\\d\\d\\d\\d\\d\\d\\d-\\d")
    patt_found = patt_found.value_counts()/df.shape[0]
    if True in patt_found.index:
        patt_found = patt_found[patt_found.index][0]
    else:
        patt_found = 0
    # Check if over threshold
    if patt_found > patt_found_th:
        res = True
    return res


# This function checks if the column defines BID of organization or supplier
# Input: DataFrame, name of the column, found final column names
# Output: The final colname of BID column


def org_or_suppl_BID(df, col, colnames):
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


def test_if_date(df, col, colnames):
    # Initialize result
    res = False
    df = df.iloc[:, colnames.index(col)]
    df = df.dropna()
    if df.dtype == "datetime64":
        res = True
    elif df.dtype == "int64":
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


def test_match_between_colnames(df, col, colnames, cols_match, datatype):
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


def test_if_sums(df, col, colnames, greater_cols, less_cols, datatype):
    # Initialize results as False
    res = False
    # Test if all cols_match are available and their type is correct
    if all((col_match in colnames for
            col_match in (greater_cols + less_cols))) and all(
                df.iloc[:, [colnames.index(col_match)
                            for col_match in (greater_cols+less_cols)
                            ]].dtypes == datatype):
        # Check if the column values are less than greater_cols values
        if len(greater_cols) > 0 and all((
                df.iloc[:, [colnames.index(greater_col)
                            for greater_col in greater_cols]].values >
                df[[df.columns[colnames.index(col)]]].values).all(axis=1)):
            less_than_great = True
        else:
            less_than_great = False
        # Check if the column values are greater than less_cols values
        if len(less_cols) > 0 and all((
                df.iloc[:, [colnames.index(less_col)
                            for less_col in less_cols]].values <
                df[[df.columns[colnames.index(col)]]].values).all(axis=1)):
            greater_than_less = True
        else:
            greater_than_less = False
        # If both were True, result is True
        if less_than_great and greater_than_less:
            res = True
    return res


# This function checks if the column defines countries
# Input: DataFrame, name of the column, found final column names
# Output: Boolean value


def test_if_country(df, col, colnames, country_th=0.2, **args):
    # INPUT CHECK
    # country_th must be numeric value 0-100
    if not ((isinstance(country_th, int) or
             isinstance(country_th, float)) and
            (0 <= country_th <= 1)):
        raise Exception(
            "'country_th' must be a number between 0-1."
            )
    # INPUT CHECK END

    # Initialize results as False
    res = False
    # Get specific column and remove NaNs
    df = df.iloc[:, colnames.index(col)]
    df = df.dropna()
    # Test if col values can be found from the table
    # CHANGE THE PATH!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    codes = pd.read_csv("~/Python/ostan/data/land_codes.csv", index_col=0)
    res_df = pd.DataFrame()
    for name, data in codes.items():
        res_df[name] = (df.isin(data))
    # How many times the value was found from the codes? If enough, then we
    # can be sure that the column includes land codes
    if sum(res_df.sum(axis=1) > 0)/res_df.shape[0] > country_th:
        res = True
    return res
