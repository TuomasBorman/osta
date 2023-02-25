#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import osta.__utils as utils
import pandas as pd
import warnings
from os import listdir
from os.path import isfile, isdir, join
import sys


def combine_data(df_list, save_file=None, log_file=False, **args):
    """
    This function merges datasets into one.

    Arguments:
        `df`: pandas.DataFrame containing invoice data.

    Details:

    Examples:
        ```

        ```

    Output:
        A pandas.DataFrame

    """
    # INPUT CHECK
    if not (isinstance(df_list, list) or isinstance(df_list, str)):
        raise Exception(
            "'df_list' must be a list of pd.DataFrames or paths to files."
            )
    if not (isinstance(save_file, str) or save_file is None):
        raise Exception(
            "'save_file' must be None or string specifying directory to ",
            "where result files will be stored."
            )
    if not (isinstance(log_file, str) or isinstance(log_file, bool)):
        raise Exception(
            "'log_file' must be a boolean value or a string specifying a path."
            )
    # If df_list is directory, check that it is correct directory
    if isinstance(df_list, str) and not isdir(df_list):
        raise Exception(
            "Directory specified by 'df_list' not found."
            )
    elif isinstance(df_list, str) and isdir(df_list):
        # If it is directory, get all the files inside it
        df_list = [join(df_list, f) for f in listdir(df_list)
                   if isfile(join(df_list, f))]
    # INPUT CHECK END
    # If user wants to create a logger file
    if log_file:
        # Create a logger with file
        logger = utils.__start_logging(__name__, log_file)

    # For progress bar, specify the width of it
    progress_bar_width = 50
    # Loop over list elements
    dfs = []
    for i, x in enumerate(df_list):
        # Update the progress bar
        percent = 100*((i+1)/len(df_list))
        i += 1
        sys.stdout.write('\r')
        sys.stdout.write("Completed: [{:{}}] {:>3}%"
                         .format('='*int(
                             percent/(100/progress_bar_width)),
                                 progress_bar_width, int(percent)))
        sys.stdout.flush()
        # Check if it is pd.DataFrame. Otherwise try to load it as a local file
        if not isinstance(x, pd.DataFrame):
            # Try to load it
            try:
                df = utils.__detect_format_and_open_file(x, **args)
            except Exception:
                msg = x if isinstance(x, str) else ("element " + i)
                warnings.warn(
                    message=f"Failed to open the file {msg}.",
                    category=UserWarning
                    )
        else:
            df = x
        # Add to list
        dfs.append(df)
    # Combine data to one DF
    df = pd.concat(dfs)
    # Stop progress bar
    sys.stdout.write("\n")
    # Save file if specified
    if save_file is not None:
        df.to_csv(save_file, index=False)
    # Reset logging; do not capture warnings anymore
    if log_file:
        utils.__stop_logging(logger)
    return df
