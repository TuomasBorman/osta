#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd


def __is_non_empty_df(df):
    res = isinstance(df, pd.DataFrame) and df.shape[0] > 0 and df.shape[1] > 0
    return res
