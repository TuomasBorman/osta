# -*- coding: utf-8 -*-
import pandas as pd
import warnings
import change_names_help_func


def change_names(df, guess_names=True, make_unique=True, fields=None, **args):
    """
    Change column names of pandas.DataFrame

    This function is used to change column names of pandas.DataFrame.
    The column names are changed into standardized format that all other
    functions in ostan package require.

    Arguments:
        df: pandas.DataFrame containing invoice data.

        guess_names: A boolean value specifying whether to guess column names
        that did not have exact matches or not. (By default: guess_names=True)

        make_unique: A boolean value specifying whether to add a suffix to
        duplicated column names. (By default: make_unique=True)

        fields: A dictionary containing matches between existing column
        names (key) and standardized names (value) or None. When fields=None,
        function's default dictionary is used. (By default: fields=None)

        **args: Additional arguments passes into other functions:

            match_th: A numeric value [0,1] specifying the threshold of enough
            good match. Value over threshold have enough strong pattern and it
            is interpreted to be a match.

            scorer: A scorer function passed into fuzzywuzzy.process.extractOne
            function. (By default: scorer=fuzz.token_sort_ratio)

            bid_patt_th: A numeric value [0,1] specifying the strength of
            business ID pattern. The observation specifies the portion of
            observations where the pattern must be present to conclude that
            column includes BIDs. (By default: bid_patt_th=0.8)

            country_code_th: A numeric value [0,1] specifying the strength of
            country code pattern. The observation specifies the portion of
            observations where the pattern must be present to conclude that
            column includes country cides. (By default: country_code_th=0.2)

    Details:
        This function changes the column names to standardized names that are
        required in other functions in ostan package. If the names are already
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

    Output:
        pandas.DataFrame with standardized column names.

    """
    # INPUT CHECK
    # df must be pandas DataFrame
    if not isinstance(df, pd.DataFrame):
        raise Exception(
            "'df' must be pandas.DataFrame."
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
    # fields must be dict or None
    if not (isinstance(fields, dict) or fields is None):
        raise Exception(
            "'fields' must be dict or None."
            )
    # INPUT CHECK END
    # Get fields / matches between column names and standardized names
    fields = change_names_help_func._get_fields_df(fields)
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
            name = change_names_help_func._guess_name(df=df,
                                                      col=col,
                                                      colnames=colnames,
                                                      fields=fields, **args)
            # if the column name was changed
            if col != name:
                # Change name
                colnames[colnames.index(col)] = name
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
    if df.columns.nunique() != df.shape[1] and make_unique:
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
