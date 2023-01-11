#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd


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
        values = values.str.split("-", expand=True)
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
