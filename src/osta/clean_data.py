#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import warnings
import numpy as np
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pkg_resources
import utils

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
    # Remove spaces from beginning and end of the value
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

    # # Check if there are duplicated column names
    # if len(set(df.columns)) != df.shape[1]:
    #     # Get unique values and their counts
    #     unique, counts = np.unique(df.columns, return_counts=True)
    #     # Get duplicated values
    #     duplicated = unique[counts > 1]
    #     # Get those duplicated values that define columns
    #     # that are being cleaned
    #     duplicated_disable = duplicated[list(dup in ["test1", "test3"]
    #                                          for dup in duplicated)]
    #     print(duplicated_disable)
    #     warnings.warn(
    #         message=f"The following column names are duplicated. "
    #         f"Please check them for errors.\n {duplicated}",
    #         category=Warning
    #         )
    # Check org_number
    df = __standardize_org(df, **args)
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
    if :
        raise Exception(
            "'org_data' must be non-empty pandas.DataFrame or None."
            )
    # INPUT CHECK END
    if org_data is None:
        path = "~/Python/osta/src/osta/resources/municipality_codes.csv"
        org_data = pd.read_csv(path, index_col=0)
    # Standardize data
    # Column that are checked from df
    cols_to_check = ["org_number", "org_name", "org_id"]
    # Column of db that are matched with columns that are being checked
    cols_to_match = ["code", "name", "bid"]
    # Standardize organization data
    df = __standardize_org_or_suppl(df=df, df_db=org_data,
                                    cols_to_check=cols_to_check,
                                    cols_to_match=cols_to_match,
                                    **args)
    return df


def __standardize_suppl(df, suppl_data=None, **args):
    """
    This function prepares supplier data to be checked, and calls
    function that checks it.
    Input: df
    Output: df with standardized supplier data
    """
    # INPUT CHECK
    if not isinstance(suppl_data, pd.DataFrame) or\
        suppl_data.shape[0] == 0 or\
            suppl_data.shape[1] == 0:
        raise Exception(
            "'org_data' must be non-empty pandas.DataFrame or None."
            )
    # INPUT CHECK END
    if suppl_data is None:
        return df
    # Standardize data
    # Column that are checked from df
    cols_to_check = ["suppl_name", "suppl_id"]
    # Column of db that are matched with columns that are being checked
    cols_to_match = ["code", "name", "bid"]
    # Standardize organization data
    df = __standardize_org_or_suppl(df=df, df_db=suppl_data,
                                    cols_to_check=cols_to_check,
                                    cols_to_match=cols_to_match,
                                    **args)
    return df

def __standardize_org_or_suppl(df, df_db,
                               cols_to_check, cols_to_match,
                               match_th=0.7, scorer=fuzz.token_sort_ratio,
                               **args):
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
    # Value [0,1] to a number between 0-100
    match_th = match_th*100
    # INPUT CHECK END
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
    # If noe matching columns were found from the data base
    if len(cols_to_check) > 0 and len(cols_to_match) == 0:
        temp = ("org_data" if re.search("org", cols_to_check[0])
                else "suppl_data")
        warnings.warn(
            message=f"'{temp}' should include at least one of the "
            "following columns: 'name' (name), 'number' "
            "(number), and 'bid' (business ID).",
            category=Warning
            )
        return df
    # If data includes organization data columns
    elif len(cols_to_check) == 0:
        return df
    # Subset the data
    df_org = df.loc[:, cols_to_check]
    # Drop duplicates so we can focus on unique rows
    org_uniq = df_org.drop_duplicates()

    # Create copy so that we can modify other
    org_uniq_mod = org_uniq.copy()
    # Initialize DF for warning messages
    df_msg = pd.DataFrame()
    not_detected = pd.DataFrame()
    part_match = pd.DataFrame()

    # Loop over rows
    for i, row in org_uniq.iterrows():
        # Get name, number and id if they are found from the df,
        # otherwise get False
        name = (row[cols_to_match.index("name")] if
                "name" in cols_to_match else False)
        number = (row[cols_to_match.index("code")] if
                  "code" in cols_to_match else False)
        bid = (row[cols_to_match.index("bid")] if
               "bid" in cols_to_match else False)
        # Can name, number and BID be found from the df_db?
        name_found = (name is not False and
                      any(df_db.loc[:, "name"].astype(str).str.lower() ==
                          name.lower()))
        number_found = (number is not False and
                        str(number).replace('.', '', 1).isdigit() and
                        any(df_db.loc[:, "code"].astype(int) == int(number)))
        bid_found = (bid is not False and any(df_db.loc[:, "bid"] == bid))

        # If bid was found
        if bid_found:
            # Take the row based on business id
            row_db = (df_db.loc[df_db.loc[:, "bid"].astype(str).str.lower()
                                == bid.lower(), cols_to_match])
            # If name or number of df do not match to row of df_db
            if ((name_found and name.lower() !=
                row_db.loc[:, "name"].astype(str).str.lower()) or
                (number_found and
                 int(bid) != int(row_db["bid"]))):
                # Get values
                temp = row.tolist()
                temp.extend(row_db.iloc[0, :].tolist())
                # Get variable names
                temp_name = row.index.tolist()
                temp_name.extend(row_db.columns.to_list())
                # Store data for warning message
                temp = pd.DataFrame(temp, index=temp_name)
                df_msg = pd.concat([df_msg, temp], ignore_index=True)
            else:
                # Add row to final data
                org_uniq_mod.iloc[i, :] = row_db
        # If name was found, it takes precedence
        elif name_found:
            # Take the row based on name
            row_db = (df_db.loc[df_db.loc[:, "name"].astype(str).str.lower()
                                == name.lower(), cols_to_match])
            # If number do not match to row (bid was not found)
            if number_found and int(number) != int(row_db["code"]):
                # Get values
                temp = row.tolist()
                temp.extend(row_db.iloc[0, :].tolist())
                # Get variable names
                temp_name = row.index.tolist()
                temp_name.extend(row_db.columns.to_list())
                # Store data for warning message
                temp = pd.DataFrame(temp, index=temp_name)
                df_msg = pd.concat([df_msg, temp], axis=1, ignore_index=True)
            else:
                # Add row to final data
                org_uniq_mod.iloc[i, :] = row_db
        # If number was found
        elif number_found:
            # Take the row based on number
            row_db = (df_db.loc[df_db.loc[:, "code"].astype(int) ==
                                int(number), cols_to_match])
            # Add row to final data (bid and name was not found)
            org_uniq_mod.iloc[i, :] = row_db
        # Test partial matching to name if name was present
        elif name is not False or name is not None:
            # Try partial match, get the most similar name
            name_part = process.extractOne(name, df_db.loc[:, "name"],
                                           scorer=scorer)
            # If the matching score is over threshold
            if name_part[1] >= match_th:
                # Get only the name
                name_part = name_part[0]
                # Find row based on name with partial match
                row_db = (df_db.loc[df_db.loc[:, "name"] == name_part,
                                    cols_to_match])
                # Add row to final data
                org_uniq_mod.iloc[i, :] = row_db
                # Store info for warning message
                temp = pd.DataFrame([name, name_part],
                                    index=[cols_to_check[
                                        cols_to_match.index("name")],
                                        "found match"])
                part_match = pd.concat([part_match, temp], axis=1)
            else:
                # Store data for warning message: data was not found
                not_detected = pd.concat([not_detected, row], axis=1)

    # If some data was not detected
    if not_detected.shape[0] > 0:
        warnings.warn(
            message=f"The following organization data "
            f"was not detected. Please check it for errors: "
            f"\n{not_detected.transpose()}",
            category=Warning
            )
    # If partial match of name was used
    if part_match.shape[0] > 0:
        warnings.warn(
            message=f"The following organization names were detected based on "
            f"partial matching: \n{part_match.transpose().drop_duplicates()}",
            category=Warning
            )

    # Which values were modified?
    # Get indices of those rows that are changed
    ind_mod = (org_uniq.fillna("") != org_uniq_mod.fillna("")).sum(axis=1) > 0
    ind_mod = [i for i, x in enumerate(ind_mod) if x]
    # Loop over those rows and replace the values of original data columns
    for i in ind_mod:
        # Get old and new rows
        old_row = org_uniq.iloc[i, :].values
        new_row = org_uniq_mod.iloc[i, :].values
        # Assign values based on bid, number or name whether they are
        # found and not None
        if ("bid" in cols_to_match
            and old_row[cols_to_match.index("bid")
                        ] is not None):
            # Get which rows of df match the value
            col = cols_to_match.index("bid")
            ind = df_org.iloc[:, col] == old_row[col]
        elif ("code" in cols_to_match and
              old_row[cols_to_match.index("code")] is not None):
            # Get which rows of df match the value
            col = cols_to_match.index("code")
            ind = df_org.iloc[:, col] == old_row[col]
        else:
            # Get which rows of df match the value
            col = cols_to_match.index("name")
            ind = df_org.iloc[:, col] == old_row[col]
        # Replace values
        df_org.loc[ind, :] = new_row
    # Assign data back to original data
    df.loc[:, cols_to_check] = df_org
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
