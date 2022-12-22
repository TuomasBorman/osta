# -*- coding: utf-8 -*-
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import warnings


def change_names(df, fields, guess_names=True, **args):
    # Open files that include fields
    mandatory_fields = pd.read_csv("~/Downloads/mandatory_fields.csv").set_index("key")["value"].to_dict()
    optional_fields = pd.read_csv("~/Downloads/optional_fields.csv").set_index("key")["value"].to_dict()
    # Combine fields into one dictionary
    fields = {}
    fields.update(mandatory_fields)
    fields.update(optional_fields)
    # Add field values as a key
    add_fields = pd.DataFrame(fields.values(), fields.values())
    add_fields = add_fields[0].to_dict()
    fields.update(add_fields)
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
        for name in colnames_not_found:
            name = guess_name(df, col, colnames, colnames_not_found.append, all_fields, **args)
            # Change name
            colnames[colnames.index(colnames_not_found[1])] = name
            # Remove from list
            colnames_not_found.remove(name)

    # Replace column names with new ones
    df.columns = colnames

    # Give warning if there were column names that were not identified
    if colnames_not_found:
        warnings.warn(
            message=f"The following column names were not detected. "
            f"Please check them for errors.\n {colnames_not_found}",
            category=UserWarning
            )
    return df


def guess_name(df, col, colnames, fields, match_th=85, scorer=fuzz.token_set_ratio, **args):
    # Try if column is ID column
    if test_if_BID(df, col):
        # BID can be from organization or supplier.
        col_name = org_or_suppl_BID(df, col)
    elif test_if_org_number(df, col, colnames):
        col_name = "org_id"
    elif test_if_date(df, col, colnames):
        col_name = "date"
    elif test_if_org_name(df, col, colnames):
        col_name = "org_name"
    elif test_if_suppl_name(df, col, colnames):
        col_name = "suppl_name"
    elif test_if_netsum(df, col, colnames):
        col_name = "price_ex_vat"
    elif test_if_total(df, col, colnames):
        col_name = "total"
    elif test_if_vat_amount(df, col, colnames):
        col_name = "vat_amount"
    else:
        # Try partial match
        # Get the most similar key value
        col_name_part = process.extractOne(col, fields.keys(),
                                           scorer=scorer)
        # If the matching score is over threshold
        if col_name_part[1] >= match_th:
            # Get only the key name
            col_name_part = col_name_part[0]
            # Based on the key, get the value
            col_name = fields.get(col_name_part)

    return col_name

def test_if_BID(df, col_name, patt_found_th=0.8, char_len_th=0.8):
    # Test if pattern found
    patt_found = df.loc[:, col_name].astype(str).str.contains(
        "\\d\\d\\d\\d\\d\\d\\d-\\d")
    patt_found = patt_found.value_counts()/df.shape[0]
    if True in patt_found.index:
        patt_found = patt_found[patt_found.index][0]
    else:
        patt_found = 0

    # Check if over threshold
    if patt_found > patt_found_th:
        res = True
    else:
        res = False

    return res


def org_or_suppl_BID(df, col, colnames):
    temp = df
    nrow = df.shape[0]
    if "org_number" in colnames:
        # Take only specified columns
        temp = temp.iloc[:, [colnames.index(col), colnames.index("org_number")] ]
        # Drop rows with blank values
        temp = temp.dropna()
        # Number of unique combinations
        n_uniq = temp.drop_duplicates().shape[0]
        # If there is only 1 unique combination, the BID is from org
        if n_uniq == 1:
            name = "org_id"
        else:
            name = "suppl_id"
    elif "org_name" in colnames:
        # Take only specified columns
        temp = temp.iloc[:, [colnames.index(col), colnames.index("org_number")] ]
        # Drop rows with blank values
        temp = temp.dropna()
        # Number of unique combinations
        n_uniq = temp.drop_duplicates().shape[0]
        # If there is only 1 unique combination, the BID is from org
        if n_uniq == 1:
            name = "org_id"
        else:
            name = "suppl_id"
    else:
        # Take only specified columns
        temp = temp.iloc[:, colnames.index(col)]
        # Drop rows with blank values
        temp = temp.dropna()
        # Number of unique combinations
        n_uniq = temp.drop_duplicates().shape[0]
        if n_uniq/nrow > 0.5:
            name = "suppl_id"
    return name


def test_if_org_number(df, col, colnames):
    temp = df
    # Test if integer
    if temp.dtypes[colnames.index(col)] == "int64":
        if "org_name" in colnames:
            # Take only specified columns
            temp = temp.iloc[:, [colnames.index(col), colnames.index("org_name")] ]
            # Drop rows with blank values
            temp = temp.dropna()
            # Number of unique combinations
            n_uniq = temp.drop_duplicates().shape[0]
            # If there is only 1 unique combination, the BID is from org
            if n_uniq == 1:
                res = True
        elif "org_id" in colnames:
            # Take only specified columns
            temp = temp.iloc[:, [colnames.index(col), colnames.index("org_id")]]
            # Drop rows with blank values
            temp = temp.dropna()
            # Number of unique combinations
            n_uniq = temp.drop_duplicates().shape[0]
            # If there is only 1 unique combination, the BID is from org
            if n_uniq == 1:
                res = True
    else:
        res = False

    return res


def test_if_date(df, col, colnames):
    temp = df
    temp = temp.dropna()
    column = temp.iloc[:, colnames.index(col)]
    if column.dtype == "datetime64":
        name = "date"
    elif column.dtype] == "int64":
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
        patt_found = df.loc[:, col].astype(str).str.contains("|".join(patt_to_search))
        if all(patt_found):
            res = True
    else:
        res = False
    return res

def test_if_org_name(df, col, colnames):
    temp = df
    temp = temp.dropna()
    if "org_number" in colnames:
        # Take only specified columns
        temp = temp.iloc[:, [colnames.index(col), colnames.index("org_number")] ]
        # Drop rows with blank values
        temp = temp.dropna()
        # Number of unique combinations
        n_uniq = temp.drop_duplicates().shape[0]
        # If there is only 1 unique combination, the BID is from org
        if n_uniq == 1:
            res = True
    elif "org_id" in colnames:
        # Take only specified columns
        temp = temp.iloc[:, [colnames.index(col), colnames.index("org_id")]]
        # Drop rows with blank values
        temp = temp.dropna()
        # Number of unique combinations
        n_uniq = temp.drop_duplicates().shape[0]
        # If there is only 1 unique combination, the BID is from org
        if n_uniq == 1:
            res = True
    else:
        res = False
    return res

def test_if_suppl_name(df, col, colnames):
    temp = df
    temp = temp.dropna()
    if "suppl_id" in colnames:
        # Take only specified columns
        temp = temp.iloc[:, [colnames.index(col), colnames.index("suppl_id")] ]
        # Drop rows with blank values
        temp = temp.dropna()
        # Number of unique combinations
        n_uniq = temp.drop_duplicates().shape[0]
        # If there is only 1 unique combination, the BID is from org
        if n_uniq == 1:
            res = True
    else:
        res = False
    return res


def test_if_netsum(df, col, colnames):
    temp = df
    temp = temp.dropna()
    res = False
    if "total" in colnames and "vat_amount" in colnames:
        temp = temp.iloc[:, [colnames.index(col), colnames.index("total"), colnames.index("vat_amount")] ]
        if temp.dtypes == "float64" and all(temp.iloc[:, colnames.index("total")] > temp.iloc[:, colnames.index(col)] & temp.iloc[:, colnames.index(col)] > temp.iloc[:, colnames.index("vat_amount")]):
            res = True
    return res

def test_if_total(df, col, colnames):
    temp = df
    temp = temp.dropna()
    res = False
    if "price_ex_vat" in colnames and "vat_amount" in colnames:
        temp = temp.iloc[:, [colnames.index(col), colnames.index("price_ex_vat"), colnames.index("vat_amount")] ]
        if temp.dtypes == "float64" and all(temp.iloc[:, colnames.index(col)] > temp.iloc[:, colnames.index("price_ex_vat")] & temp.iloc[:, colnames.index(col)] > temp.iloc[:, colnames.index("vat_amount")]):
            res = True
    return res

def test_if_vat_amount(df, col, colnames):
    temp = df
    temp = temp.dropna()
    res = False
    if "price_ex_vat" in colnames and "total" in colnames:
        temp = temp.iloc[:, [colnames.index(col), colnames.index("price_ex_vat"), colnames.index("total")] ]
        if temp.dtypes == "float64" and all(temp.iloc[:, colnames.index(col)] < temp.iloc[:, colnames.index("price_ex_vat")] & temp.iloc[:, colnames.index(col)] < temp.iloc[:, colnames.index("total")]):
            res = True
    return res