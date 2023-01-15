#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import re


def add_second_cat1(df, account_col):
    # Add account category 2 to own column
    df = df.assign(account_cat2=None)
    row_i = df.loc[:, account_col].isnull()
    df.loc[row_i, "account_cat2"] = df.iloc[df.index[row_i], 0]
    df.loc[:, "account_cat2"] = df.loc[:, "account_cat2"].fillna(
        method="ffill")
    df = df.drop(df.index[row_i])
    df.reset_index(drop=True, inplace=True)
    return df


def add_second_cat2(df):
    # Add 2nd account category to own column
    # Get first occurances of "vastaava" and "vastattavaa"
    # and lasts which are "* yhteensä"
    patt = "vastaavaa"
    ind = df.iloc[:, 0].astype(str).str.contains(patt, flags=re.IGNORECASE)
    ind1 = df.loc[ind, :].first_valid_index()
    ind2 = df.loc[ind, :].last_valid_index()
    patt = "vastattavaa"
    ind = df.iloc[:, 0].astype(str).str.contains(patt, flags=re.IGNORECASE)
    ind3 = df.loc[ind, :].first_valid_index()
    ind4 = df.loc[ind, :].last_valid_index()
    # Add data
    df = df.assign(account_cat2=None)
    df.loc[ind1, "account_cat2"] = df.iloc[df.index[ind1], 0]
    df.loc[ind3, "account_cat2"] = df.iloc[df.index[ind3], 0]
    df.loc[:, "account_cat2"] = df.loc[:, "account_cat2"].fillna(
        method="ffill")
    df = df.drop([ind1, ind2, ind3, ind4])
    df.reset_index(drop=True, inplace=True)
    return df


def add_third_cat(df, account_col):
    # Add account category 3 to own column
    df = df.assign(account_cat3=None)
    patt = [
        "\\d\\d\\d\\d[‒\\-]\\d\\d\\d\\d",
        "\\d\\d\\d\\d [‒\\-] \\d\\d\\d\\d",
        ]
    row_i = df.loc[:, account_col].astype(str).str.contains(
        "|".join(patt))
    # Or find sttrings with uppercase tasetilissä uppercasetasot
    # row_i = row_i or df.iloc[:, 0].str.upper() == df.iloc[:, 0].str
    df.loc[row_i, "account_cat3"] = df.iloc[df.index[row_i], 0]
    df.loc[:, "account_cat3"] = df.loc[:, "account_cat3"].fillna(
        method="ffill")
    df = df.drop(df.index[row_i])
    df.reset_index(drop=True, inplace=True)
    return df


def add_third_cat2(df, account_col):
    # Add account category 3 to own column
    df = df.assign(account_cat3=None)
    # Find trings with uppercase
    row_i = df.iloc[:, 0].str.upper() == df.iloc[:, 0]
    df.loc[row_i, "account_cat3"] = df.iloc[df.index[row_i], 0]
    df.loc[:, "account_cat3"] = df.loc[:, "account_cat3"].fillna(
        method="ffill")
    df = df.drop(df.index[row_i])
    df.reset_index(drop=True, inplace=True)
    return df


def add_tase_cats(df, account_col, cat_i):
    # Get onlu higher level categories from the data
    patt = [
        "\\d\\d\\d\\d[‒\\-]\\d\\d\\d\\d",
        "\\d\\d\\d\\d [‒\\-] \\d\\d\\d\\d",
        ]
    row_i = df.loc[:, account_col].astype(str).str.contains(
        "|".join(patt))
    cats = df.iloc[df.index[row_i], :]
    # Combine data to one column
    cats = add_account_name(cats)
    # Remove higher level categories from the original data
    df = df.loc[-row_i, :]
    # Get only account numbers from the data, and split them to
    # start and end index
    val = cats.loc[:, account_col].astype(str).str.split("[‒\\-]", expand=True)
    val = val.astype(int)
    # Add to data
    cats[["ind1", "ind2"]] = val
    # Initialize DF for results
    df = df.dropna(subset=account_col)
    df[account_col] = df[account_col].astype(int)
    df_cat = pd.DataFrame(index=df[account_col])
    # Loop over higher level categories
    for i in cats.index:
        # Get indices of correct values
        ind = (df_cat.index >= cats.loc[cats.index[i], "ind1"]) & (
            df_cat.index <= cats.loc[cats.index[i], "ind2"])
        # Create as many categories needed to fill the values
        cat_i_temp = cat_i
        true_value = True
        while true_value:
            # Get category column name
            temp_i = "cat_" + str(cat_i_temp)
            # If the column does not exist yet
            if temp_i not in df_cat.columns:
                df_cat = df_cat.assign(temp=None)
                df_cat = df_cat.rename(columns={"temp": temp_i})
            # Get category values
            temp = df_cat.loc[ind, temp_i]
            # If category is empty
            if all(x is None for x in temp):
                # Add category tot this level and continue with other
                # categories
                df_cat.loc[ind, temp_i] = cats.loc[i, "account_name"]
                true_value = False
                cat_i_temp = cat_i
            else:
                # Else put the category to next level
                cat_i_temp = cat_i_temp + 1
    # Add to original DF based on account number
    if not all(df_cat.index == df[account_col]):
        raise Exception(
            "Error."
            )
    df_cat.reset_index(inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = df.merge(df_cat, on=account_col)
    return df


def add_account_name(df):
    # Add account name by combining all the rest of the values
    df = df.dropna(how="all", axis=1)
    col_i = max([i for i, x in enumerate(df.columns) if x is None])
    df.loc[:, "account_name"] = df.iloc[:, 0:col_i+1].fillna(
        "").astype(str).agg(''.join, axis=1)
    df = df.iloc[:, col_i+1:]
    df.reset_index(drop=True, inplace=True)
    return df


def adjust_columns(df, col_to_search, match_th, scorer):
    # Find the first column
    for i in range(1, df.shape[0]):
        temp = process.extractOne(col_to_search, df.columns, scorer=scorer)
        if temp[1] >= match_th:
            break
        df.columns = df.iloc[0, :]
        df = df.drop([df.index[0]])
    df.reset_index(drop=True, inplace=True)
    return df


def get_accounts(file, sheet_name, year,
                 match_th=80, scorer=fuzz.token_sort_ratio):
    # Read file
    df = pd.read_excel(path, sheet_name=sheet_name,
                       index_col=None, header=None)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Replace NaN with None
    df = df.replace(np.nan, None)
    # File contains two sets of accounts; find both
    text_to_search = "tuloslaskelman tilit"
    temp = process.extractOne(text_to_search, df.iloc[:, 0], scorer=scorer)
    # If the matching score is over threshold
    if temp[1] >= 80:
        # Get only the column name
        temp = temp[0]
        ind = df.index[df.iloc[:, 0] == temp][0]
    else:
        raise Exception(
            "'Tuloslaskelman tilit' row was not found."
            )
    text_to_search = "tasetilit"
    temp2 = process.extractOne(text_to_search, df.iloc[:, 0], scorer=scorer)
    # If the matching score is over threshold
    if temp2[1] >= 80:
        # Get only the column name
        temp2 = temp2[0]
        ind2 = df.index[df.iloc[:, 0] == temp2][0]
    else:
        raise Exception(
            "'Tasetilit' row was not found."
            )
    # Subset the data by taking data from the start of 1st set of values
    df1 = df.iloc[ind:ind2, ]
    df1.columns = df1.iloc[0, :]
    df1 = df1.drop([df1.index[0]])
    # Subset the data by taking data from the start of the 2nd set of values
    df2 = df.iloc[ind2:, ]
    df2.columns = df2.iloc[0, :]
    df2 = df2.drop([df2.index[0]])
    # Get column that includes account numbers
    col_to_search = "tilin numero"
    # Adjust column names, find the first row
    df1 = adjust_columns(df1, col_to_search, match_th, scorer)
    df2 = adjust_columns(df2, col_to_search, match_th, scorer)
    account_col = process.extractOne(col_to_search, df1.columns, scorer=scorer)
    # Get only the column name
    account_col = account_col[0]
    # Polish data sets
    df1 = df1.dropna(how="all", axis=0)
    df2 = df2.dropna(how="all", axis=0)
    df1 = df1.dropna(how="all", axis=1)
    df2 = df2.dropna(how="all", axis=1)
    df1.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    # Add categories for 1st set
    df1 = df1.assign(account_cat1="tuloslaskelma")
    df1 = add_second_cat1(df1, account_col)
    df1 = add_third_cat(df1, account_col)
    df1 = add_account_name(df1)
    # Add categories for 2nd set
    df2 = df2.assign(account_cat1="tase")
    df2 = add_second_cat2(df2)
    df2 = add_third_cat2(df2, account_col)
    df2 = add_tase_cats(df2, account_col, 4)
    df2 = add_account_name(df2)
    # Add year
    df1 = df1.assign(year=year)
    df2 = df2.assign(year=year)
    # Combine accounts
    df = pd.concat([df1, df2])
    df = df.reset_index(drop=True)
    return df


# Patt tho website
path = ("https://vkazprodwordpressstacc01.blob.core.windows.net/" +
        "wordpress/2020/04/Liite-1.-Tililuettelo-Excel-3.6.2022.xlsx")

# 2021
sheet = "Tililuettelo tilikaudelle 2021"
df_2021 = get_accounts(file=path, sheet_name=sheet, year=2021)
columns = [
    "number",
    "info",
    "cat1",
    "cat2",
    "cat3",
    "name",
    "year",
    "cat4",
    "cat5",
    "cat6",
    ]
df_2021.columns = columns

# 2022
sheet = "Tililuettelo tilikaudelle 2022"
df_2022 = get_accounts(file=path, sheet_name=sheet, year=2022)
columns = [
    "number",
    "info",
    "cat1",
    "cat2",
    "cat3",
    "name",
    "year",
    "cat4",
    "cat5",
    "cat6",
    ]
df_2022.columns = columns

# 2023
sheet = "Tililuettelo tilikaudelle 2023"
df_2023 = get_accounts(file=path, sheet_name=sheet, year=2023)
columns = [
    "number",
    "info",
    "cat1",
    "cat2",
    "cat3",
    "name",
    "year",
    "cat4",
    "cat5",
    "cat6",
    ]
df_2023.columns = columns

# Combine result
df = pd.concat([df_2021, df_2022, df_2023])

# Order columns
columns = [
    "number",
    "name",
    "cat1",
    "cat2",
    "cat3",
    "cat4",
    "cat5",
    "cat6",
    "info",
    "year"
    ]
df = df.loc[:, columns]

# Remove spaces from beginning and end of the value
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# Order data, the newest year first
df = df.sort_values("year", ascending=False)
# Run in data folder
path = "../src/osta/resources/account_info.csv"
df.to_csv(path)
