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
