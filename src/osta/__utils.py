#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import re


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
