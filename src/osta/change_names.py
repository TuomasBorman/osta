# -*- coding: utf-8 -*-
import osta.__utils as utils
import pandas as pd
import warnings
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pkg_resources


def change_names(df, guess_names=True, make_unique=True, fields=None, **args):
    """
    Change column names of pandas.DataFrame

    This function is used to change column names of pandas.DataFrame.
    The column names are changed into standardized format that all other
    functions in osta package require.

    Arguments:
        `df`: pandas.DataFrame containing invoice data.

        `guess_names`: A boolean value specifying whether to guess column names
        that did not have exact matches or not. (By default: guess_names=True)

        `make_unique`: A boolean value specifying whether to add a suffix to
        duplicated column names. (By default: make_unique=True)

        `fields`: A pandas.DataFrame or a dictionary containing
        matches between existing column names (key) and
        standardized names (value), a string specifying a path
        to such CSV file or None. When fields=None,function's
        default dictionary is used. (By default: fields=None)

        `**args`: Additional arguments passes into other functions:

        `pattern_th`: A numeric value [0,1] specifying the threshold of
        enough good match. Value over threshold have enough strong
        pattern and it is interpreted to be a match.
        (By default: pattern_th=0.9)

        `scorer`: A scorer function passed into fuzzywuzzy.process.extractOne
        function. (By default: scorer=fuzz.token_sort_ratio)

        `match_th`: A numeric value [0,1] specifying the strength of
        pattern in the data. The observation specifies the portion of
        observations where the pattern must be present to conclude that
        column includes specific type of data. (By default: match_th=0.2)

    Details:
        This function changes the column names to standardized names that are
        required in other functions in osta package. If the names are already
        in standardized format, the function works as checker as it gives
        warnings if certain name was not detected or it has been changed with
        non-exact match.

        First, the function checks if exact match is found between existing
        column names and names in dictionary. If the natch was found, certain
        column name is replaced with standardizd name.

        Secondly, if there are column names that did not have exact match, user
        can specify whether matches are checked more loosely. The function
        checks if a pattern of values of undetected column matches with pattern
        of certain data types that are expected to be in invoice data. The
        checking is done with "fail-safe" principle to be sure that matches are
        found with the highest possible accuracy.

        If a match is not detected, the function tries to find if
        the column name resembles one of the names in dictionary.
        If the signal is strong enough, the column name is is changed.
        Otherwise, the column name stays unmodified.

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
        # Change those column names that are detected based on column name
        # and pattern of the data.
        df = change_names(df)

        # To disable name guessing, use guess_names=False
        df = change_names(df, guess_names=False)

        # To control name matching, feed arguments
        df = change_names(df, guess_names=True, make_unique=True,
                          pattern_th=0.6, match_th=1)
        ```

    Output:
        pandas.DataFrame with standardized column names.

    """
    # INPUT CHECK
    # df must be pandas DataFrame
    if not utils.__is_non_empty_df(df):
        raise Exception(
            "'df' must be non-empty pandas.DataFrame."
            )
    # guess_names must be boolean
    if not isinstance(guess_names, bool):
        raise Exception(
            "'guess_names' must be bool."
            )
    # make_unique must be boolean
    if not isinstance(make_unique, bool):
        raise Exception(
            "'make_unique' must be bool."
            )
    # fields must be DataFrame or None
    if not (utils.__is_non_empty_df(fields) or
            isinstance(fields, dict) or isinstance(fields, str)
            or fields is None):
        raise Exception(
            "'fields' must be pd.DataFrame, dict, string or None."
            )
    # INPUT CHECK END
    # Get fields / matches between column names and standardized names
    fields = __get_fields_df(fields)
    # Initialize lists for column names
    colnames = []
    colnames_not_found = []
    colnames_not_found_i = []
    # Loop over column names
    for i, col in enumerate(df.columns):
        # Column name to lower case and remove spaces from beginning and end
        # Get matching value from the dictionary
        col_name = fields.get(col.lower().strip())
        # If exact match was not found
        if col_name is None:
            # Do not change the colum name,
            # and add it to not-found column names list
            col_name = col
            colnames_not_found.append(col_name)
            colnames_not_found_i.append(i)
        # Append the list of column names
        colnames.append(col_name)

    # If there are column names that were not detected and user wants them
    # to be guessed
    if len(colnames_not_found) > 0 and guess_names:
        # Initialize list for new and old column names for warning message
        colnames_old = []
        colnames_new = []
        for i in colnames_not_found_i:
            col = df.columns[i]
            name = __guess_name(df=df,
                                col_i=i,
                                colnames=colnames,
                                fields=fields, **args)
            # if the column name was changed
            if col != name:
                # Change name
                colnames[i] = name
                # Append old and new column name list
                colnames_old.append(col)
                colnames_new.append(name)
        # If there are columns that were changed, give warning
        if len(colnames_new) > 0:
            warnings.warn(
                message=f"The following column names... \n {colnames_old}\n"
                f"... were replaced with \n {colnames_new}",
                category=Warning
                )
        # Update not-found column names
        colnames_not_found = [i for i in colnames_not_found
                              if i not in colnames_old]
    # Replace column names with new ones
    df.columns = colnames

    # Give warning if there were column names that were not identified
    if len(colnames_not_found) > 0:
        warnings.warn(
            message=f"The following column names were not detected. "
            f"Please check them for errors.\n {colnames_not_found}",
            category=Warning
            )

    # If there are duplicated column names and user want to make them unique
    if len(set(df.columns)) != df.shape[1] and make_unique:
        # Initialize a list for new column names
        colnames = []
        colnames_old = []
        colnames_new = []
        # Loop over column names
        for col in df.columns:
            # If there are already column that has same name
            if col in colnames:
                # Add old name to list
                colnames_old.append(col)
                # Add suffix to name
                col = col + "_" + str(colnames.count(col)+1)
                # Add new column name to list
                colnames_new.append(col)
            # Add column name to list
            colnames.append(col)
        # Give warning
        warnings.warn(
            message=f"The following duplicated column names... \n"
            f"{colnames_old}\n... were replaced with \n {colnames_new}",
            category=Warning
            )
        # Replace column names with new ones
        df.columns = colnames
    return df

# HELP FUNCTIONS


def __get_fields_df(fields, **args):
    """
    Fetch dictionary that contains fields and create a dictionary from it.
    Input: A DataFrame or dictionary of fields or nothing.
    Output: A dictionary containing which column names are meaning the same
    """
    # If fields was not provided, open files that include fields
    if fields is None:
        # Load data from /resources of package osta
        path = pkg_resources.resource_filename(
            "osta", "resources/" + "mandatory_fields.csv")
        mandatory_fields = pd.read_csv(path
                                       ).set_index("key")["value"].to_dict()
        path = pkg_resources.resource_filename(
            "osta", "resources/" + "optional_fields.csv")
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
    elif isinstance(fields, pd.DataFrame):
        # If fields does not include key and values
        if not all([i in fields.columns for i in ["key", "value"]]):
            raise Exception(
                "'fields' must include columns 'key' and 'value'."
                )
        # Convert DF to dict
        fields = fields.set_index("key")["value"].to_dict()
    elif isinstance(fields, str):
        # Read data based on path
        fields = pd.read_csv(fields, **args)
        # If fields does not include key and values
        if not all([i in fields.columns for i in ["key", "value"]]):
            raise Exception(
                "'fields' must include columns 'key' and 'value'."
                )
        # Convert DF to dict
        fields = fields.set_index("key")["value"].to_dict()
    # The search is case insensitive --> make keys lowercase
    # The key will be matched to value that is made lowercase
    fields = {k.lower(): v for k, v in fields.items()}
    return fields


def __guess_name(df, col_i, colnames, fields, pattern_th=0.9, match_th=0.8,
                 **args):
    """
    Guess column names based on pattern.
    Input: DataFrame, index of column being guesses,
    current column names, match
    between column names and standardized names.
    Output: A guessed column name
    """
    # INPUT CHECK
    # Types of all other arguments are fixed
    # pattern_th must be numeric value 0-1
    if not utils.__is_percentage(pattern_th):
        raise Exception(
            "'pattern_th' must be a number between 0-1."
            )
    # match_th must be numeric value 0-100
    if not utils.__is_percentage(match_th):
        raise Exception(
            "'match_th' must be a number between 0-1."
            )
    # INPUT CHECK END
    # Remove spaces from beginning and end of the values
    df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Get the name of the column
    col = df.columns[col_i]

    # Try strict loose match (0.95) if pattern_th is smaller than 0.95
    pattern_th_strict = 0.95 if pattern_th <= 0.95 else pattern_th
    res = __test_if_loose_match(col=col, fields=fields,
                                pattern_th=pattern_th_strict, **args)
    # If there were match, column is renamed
    if res != col:
        col = res
    # Try if column is ID column
    elif __test_if_BID(df=df, col_i=col_i, match_th=match_th):
        # BID can be from organization or supplier
        col = __org_or_suppl_BID(df=df, col_i=col_i, colnames=colnames,
                                 match_th=match_th)
    # Test if date
    elif utils.__test_if_date(df=df.iloc[:, col_i]):
        col = "date"
    # Test if column includes country codes
    elif __test_if_in_db(df=df, col_i=col_i, colnames=colnames,
                         db_file="land_codes.csv",
                         test="country", match_th=match_th,
                         **args):
        col = "country"
    # Test if column includes VAT numbers
    elif __test_if_vat_number(df=df, col_i=col_i, colnames=colnames,
                              match_th=match_th):
        col = "vat_number"
    # Test if org_name
    elif __test_if_in_db(df=df, col_i=col_i, colnames=colnames,
                         db_file="municipality_codes.csv",
                         test="name", match_th=match_th,
                         cols_not_match=["suppl_name", "suppl_number"],
                         cols_to_match=["org_number", "org_id"],
                         datatype=["object"],
                         **args):
        col = "org_name"
    # Test if service_cat
    elif __test_if_in_db(df=df, col_i=col_i, colnames=colnames,
                         db_file="service_codes.csv",
                         test="number", match_th=match_th,
                         do_not_match=["account_number", "account_name"],
                         datatype=["int64"],
                         **args):
        col = "service_cat"
    # Test if service_cat_name
    elif __test_if_in_db(df=df, col_i=col_i, colnames=colnames,
                         db_file="service_codes.csv",
                         test="name", match_th=match_th,
                         do_not_match=["account_number", "account_name"],
                         datatype=["object"],
                         **args):
        col = "service_cat_name"
    # Test if account_number
    elif __test_if_in_db(df=df, col_i=col_i, colnames=colnames,
                         db_file="account_info.csv",
                         test="number", match_th=match_th,
                         do_not_match=["service_cat", "service_cat_name"],
                         datatype=["int64"],
                         **args):
        col = "account_number"
    # Test if account_name
    elif __test_if_in_db(df=df, col_i=col_i, colnames=colnames,
                         db_file="account_info.csv",
                         test="name", match_th=match_th,
                         do_not_match=["service_cat", "service_cat_name"],
                         datatype=["object"],
                         **args):
        col = "account_name"
    # # Test if org_number
    elif __test_match_between_colnames(df=df, col_i=col_i, colnames=colnames,
                                       cols_match=["org_name", "org_id"],
                                       datatype=["int64"]
                                       ):
        col = "org_number"
    # Test if suppl_name
    elif __test_match_between_colnames(df=df, col_i=col_i, colnames=colnames,
                                       cols_match=["suppl_id"],
                                       datatype=["object"]
                                       ):
        col = "suppl_name"
    # test if price_ex_vat
    elif __test_if_sums(df=df, col_i=col_i, colnames=colnames,
                        test_sum="price_ex_vat",
                        match_with=["total", "vat_amount"],
                        datatype="float64"
                        ):
        col = "price_ex_vat"
    # test if total
    elif __test_if_sums(df=df, col_i=col_i, colnames=colnames,
                        test_sum="total",
                        match_with=["vat_amount", "price_ex_vat"],
                        datatype="float64"
                        ):
        col = "total"
    # test if vat_amount
    elif __test_if_sums(df=df, col_i=col_i, colnames=colnames,
                        test_sum="vat_amount",
                        match_with=["total", "price_ex_vat"],
                        datatype="float64"
                        ):
        col = "vat_amount"
    # Test if voucher
    elif utils.__test_if_voucher(df=df, col_i=col_i, colnames=colnames):
        col = "voucher"
    else:
        # Get match from partial matching
        col = __test_if_loose_match(col=col, fields=fields,
                                    pattern_th=pattern_th, **args)
    return col


def __test_if_loose_match(col, fields, pattern_th,
                          scorer=fuzz.token_sort_ratio,
                          **args):
    """
    Guess column names based on pattern on it.
    Input: Column name and names that are tried to be match with it
    Output: A guessed column name
    """
    # If column is not empty
    if col.strip():
        # Try partial match, get the most similar key value
        col_name_part = process.extractOne(col, fields.keys(),
                                           scorer=scorer)
        # Value [0,1] to a number between 0-100
        pattern_th = pattern_th*100
        # If the matching score is over threshold
        if col_name_part[1] >= pattern_th:
            # Get only the key name
            col_name_part = col_name_part[0]
            # Based on the key, get the value
            col = fields.get(col_name_part)
    return col


def __test_if_BID(df, col_i, match_th):
    """
    This function checks if the column defines BIDs (y-tunnus)
    Input: DataFrame, index of the column, found final column names
    Output: Boolean value
    """
    # Initialize result as False
    res = False
    # Test if pattern found
    patt_found = utils.__are_valid_bids(df.iloc[:, col_i])
    patt_found = patt_found.value_counts()/df.shape[0]
    # Test of length correct
    len_correct = df.iloc[:, col_i].astype(str).str.len() == 9
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
    if patt_found >= match_th:
        res = True
    return res


def __org_or_suppl_BID(df, col_i, colnames, match_th):
    """
    This function checks if the column defines BID of organization or supplier
    Input: DataFrame, index of the column, found final column names
    Output: The final colname of BID column
    """
    # If BID can be found from the database
    if __test_if_in_db(df=df, col_i=col_i, colnames=colnames,
                       db_file="municipality_codes.csv",
                       test="bid", match_th=match_th):
        res = "org_bid"
    else:
        # Initialize result as supplier ID
        res = "suppl_id"
        # List of columns that are matched
        cols_match = ["org_number", "org_name"]
        # Loop over columns that should be matched
        for col_match in cols_match:
            # If the column is in colnames
            if col_match in colnames:
                # Subset the data by taking only specified columns
                temp = df.iloc[:, [col_i, colnames.index(col_match)]]
                # Drop rows with blank values
                temp = temp.dropna()
                # Number of unique combinations
                n_uniq = temp.drop_duplicates().shape[0]
                # If there are as many combinations as there are
                # individual values these columns match
                if n_uniq == df.iloc[:, colnames.index(col_match)].nunique():
                    res = "org_id"
        # If all the identifiers are missing, give "bid", because we
        # cannot be sure
        if all(list(name not in colnames for name in ["org_number", "org_name",
                                                      "org_id", "suppl_name",
                                                      "suppl_id"])):
            res = "bid"
        # If there are supplier IDs already, try if they are differemt
        if "suppl_id" in colnames and all(df.iloc[:, col_i] !=
                                          df.iloc[:, colnames.index(
                                              "suppl_id")]):
            res = "org_id"
        # If there are organization IDs already, try if they are differemt
        if "org_id" in colnames and all(df.iloc[:, col_i] ==
                                        df.iloc[:, colnames.index("org_id")]):
            res = "org_id"
        # If there are not many unique values, it might be organization ID
        if df.iloc[:, col_i].nunique()/df.shape[0] < 0.5:
            res = "org_id"
    return res


def __test_match_between_colnames(df, col_i, colnames, cols_match, datatype):
    """
    This function checks if the column defines extra information of
    another column / if the column is related to that
    Input: DataFrame, index of the column, found final column names
    Output: Boolean value
    """
    # Initialize results as False
    res = False
    # Test the data type
    if df.dtypes[col_i] in datatype:
        # Loop over columns that should be matched
        for col_match in cols_match:
            # If the column is in colnames
            if col_match in colnames:
                # Subset the data by taking only specified columns
                temp = df.iloc[:, [col_i, colnames.index(col_match)]]
                # Drop rows with blank values
                temp = temp.dropna()
                # Number of unique combinations
                n_uniq = temp.drop_duplicates().shape[0]
                # If there are as many combinations as there are
                # individual values these columns match
                if n_uniq == df.iloc[:, colnames.index(col_match)].nunique():
                    res = True
    return res


def __test_if_sums(df, col_i, colnames, test_sum, match_with, datatype):
    """
    This function checks if the column defines total, net, or VAT sum,
    the arguments defines what is searched
    Input: DataFrame, index of the column, found final column names
    Output: Boolean value
    """
    # Initialize results as False
    res = False
    # If all columns are available
    if all(mw in colnames for mw in match_with):
        # Take only specific columns
        ind = list(colnames.index(mw) for mw in match_with)
        ind.append(col_i)
        # Preserve the order of indices
        ind.sort()
        df = df.iloc[:, ind]
        # Drop empty rows
        df = df.dropna()
        # If the datatypes are correct
        if all(df.dtypes == datatype):
            # If VAT is tested and value is correct
            if test_sum == "vat_amount" and\
                all(df.iloc[:, col_i] ==
                    df.iloc[:, colnames.index("total")] -
                    df.iloc[:, colnames.index("price_ex_vat")]):
                res = True
            # If total is tested and value is correct
            elif test_sum == "total" and\
                all(df.iloc[:, col_i] ==
                    df.iloc[:, colnames.index("price_ex_vat")] +
                    df.iloc[:, colnames.index("vat_amount")]):
                res = True
            # If price_ex_vat is tested and value is correct
            elif test_sum == "price_ex_vat" and\
                all(df.iloc[:, col_i] ==
                    df.iloc[:, colnames.index("total")] -
                    df.iloc[:, colnames.index("vat_amount")]):
                res = True
    return res


def __test_if_vat_number(df, col_i, colnames, match_th):
    """
    This function checks if the column defines VAT numbers
    Input: DataFrame, index of the column, found final column names
    Output: Boolean value
    """
    # Initialize result
    res = False
    # Get specific column and remove NaNs
    df = df.iloc[:, col_i]
    nrow = df.shape[0]
    df = df.dropna()
    # Check if values are VAT numbers
    res_patt = utils.__are_valid_vat_numbers(df)
    # How many times the pattern was found from values? If enough, then we
    # can be sure that the column includes VAT numbers
    if sum(res_patt)/nrow >= match_th:
        res = True
    return res


def __test_if_in_db(df, col_i, colnames, test, db_file, match_th,
                    datatype=None, cols_not_match=None, cols_to_match=None,
                    **args):
    """
    This function tests if the column includes account or service category info
    Input: DataFrame, index of the column, found final column names, account
    or service data type to search
    Output: Boolean value
    """
    # Initialize results as False
    res = False
    res2 = False
    res3 = False
    # Check that the column does not match with other specified columns
    if cols_not_match is not None:
        res2 = __test_match_between_colnames(df=df, col_i=col_i,
                                             colnames=colnames,
                                             cols_match=cols_not_match,
                                             datatype=datatype)
    # Check if other columns that specify same instance are found
    if cols_to_match is not None:
        if cols_to_match is not None:
            res3 = __test_match_between_colnames(df=df, col_i=col_i,
                                                 colnames=colnames,
                                                 cols_match=cols_to_match,
                                                 datatype=datatype)
    # Get specific column and remove NaNs
    df = df.iloc[:, col_i]
    df = df.dropna()
    df.drop_duplicates()
    # Does the column include integers
    res_list = df.astype(str).str.isdigit()
    if ((any(res_list) and test == "number") or (all(
            -res_list) and test != "number")):
        # Test if col values can be found from the table
        # Load codes from resources of package osta
        path = pkg_resources.resource_filename(
            "osta",
            "resources/" + db_file)
        db = pd.read_csv(path, index_col=0)
        # If countries, take whole data, otherwise get only specific column
        if test == "country":
            db = db.drop("code_num", axis=1)
        else:
            db = db[[test]]
        # Initialize a data frame
        df_res = pd.DataFrame()
        # Loop over columns of database
        for i, data in db.items():
            # Does the column include certain codes?
            if data.dtype == "object" and df.dtype == "object":
                temp = df.astype(str).str.lower().isin(
                    data.astype(str).str.lower())
            else:
                temp = df.isin(data)
            df_res[i] = temp
        # How many times the value was found from the codes? If enough, then we
        # can be sure that the column includes land codes
        if sum(df_res.sum(axis=1) > 0)/df_res.shape[0] >= match_th:
            res = True
    # Combine result
    if res2 is False and (res or res3):
        res = True
    else:
        res = False
    return res
