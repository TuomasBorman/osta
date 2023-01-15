#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd

# Website address
path = ("https://vkazprodwordpressstacc01.blob.core.windows.net" +
        "/wordpress/2020/04/Liite-5.-Kunnan-ja-kuntayhtyman-" +
        "palveluluokitus-Excel-14.12.2022.xlsx")


def read_service_code(path, sheet_name, year, column_names=None):
    # Read file
    df = pd.read_excel(path, sheet_name=sheet_name)
    # Drop empty rows and columns
    df = df.dropna(axis=0, thresh=2)
    df = df.dropna(axis=1, thresh=2)
    # If first rows were additional information, adjust column names
    if all(df.index != 0):
        df.columns = df.iloc[0, :]
        df.drop([df.index[0]], inplace=True)
    # Reset index
    df.reset_index(inplace=True, drop=True)
    # Add year
    df["service_cat_year"] = year
    # Change columns
    if column_names is not None:
        df.columns = column_names
    return df


# 2021 service category
cols_2021 = [
    "service_cat_name_high",
    "service_cat",
    "service_cat_name",
    "service_cat_name_short",
    "service_desc",
    "service_info",
    "service_info2",
    "service_cat_year"
    ]
df_2021 = read_service_code(path, sheet_name="Palveluluokitus 2021",
                            column_names=cols_2021, year=2021)

# 2022 service category
cols_2022 = [
    "service_cat_name_high",
    "service_cat",
    "service_cat_name",
    "service_desc",
    "service_info",
    "service_info2",
    "service_cat_year"
    ]
df_2022 = read_service_code(path, sheet_name="Palveluluokitus 2022",
                            column_names=cols_2022, year=2022)

# 2023 service category
cols_2023 = [
    "service_cat",
    "service_cat_name",
    "service_desc",
    "service_info",
    "service_info2",
    "service_cat_year"
    ]
df_2023 = read_service_code(path, sheet_name="Palveluluokitus 2023",
                            column_names=cols_2023, year=2023)

# Combine all DFs
df = pd.concat([df_2021, df_2022, df_2023])

# Rename
columns = [
    "name_higher",
    "number",
    "name",
    "short",
    "desc",
    "info",
    "info2",
    "year"
    ]
df.columns = columns
# Reorder
columns = [
    "number",
    "name",
    "name_higher",
    "short",
    "desc",
    "info",
    "info2",
    "year"
    ]
df = df.loc[:, columns]

# Order data, the newest year first
df = df.sort_values(["year", "number"], ascending=[False, True])
df.reset_index(drop=True, inplace=True)
# Run in project root folder
path = "../src/osta/resources/service_codes.csv"
df.to_csv(path)
