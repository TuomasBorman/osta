#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd

# Website address
path = ("https://vkazprodwordpressstacc01.blob.core.windows.net" +
        "/wordpress/2020/04/Liite-5.-Kunnan-ja-kuntayhtyman-" +
        "palveluluokitus-Excel-14.12.2022.xlsx")

# 2021 service category
df_2021 = pd.read_excel(path, sheet_name="Palveluluokitus 2021")
# Drop empty rows and columns
df_2021 = df_2021.dropna(axis=0, thresh=df_2021.shape[1]-2)
df_2021 = df_2021.dropna(axis=1, thresh=df_2021.shape[1]-2)
# If first rows were additional information, adjust column names
if all(df_2021.index != 0):
    df_2021.columns = df_2021.iloc[0, :]
    df_2021.drop([df_2021.index[0]], inplace=True)
df_2021.reset_index(inplace=True, drop=True)

# 2022 service category
df_2022 = pd.read_excel(path, sheet_name="Palveluluokitus 2022")
# Drop empty rows and columns
df_2022 = df_2022.dropna(axis=0, thresh=df_2021.shape[1]-2)
df_2022 = df_2022.dropna(axis=1, thresh=df_2021.shape[1]-2)
# If first rows were additional information, adjust column names
if all(df_2022.index != 0):
    df_2022.columns = df_2022.iloc[0, :]
    df_2022.drop([df_2022.index[0]], inplace=True)
df_2022.reset_index(inplace=True, drop=True)

# 2023 service category
df_2023 = pd.read_excel(path, sheet_name="Palveluluokitus 2022")
# Drop empty rows and columns
df_2023 = df_2023.dropna(axis=0, thresh=df_2021.shape[1]-2)
df_2023 = df_2023.dropna(axis=1, thresh=df_2021.shape[1]-2)
# If first rows were additional information, adjust column names
if all(df_2023.index != 0):
    df_2023.columns = df_2023.iloc[0, :]
    df_2023.drop([df_2023.index[0]], inplace=True)
df_2023.reset_index(inplace=True, drop=True)

# Change column names
df_2021.columns = [
    "service_cat_name_high",
    "service_cat",
    "service_cat_name",
    "service_cat_name_short",
    "service_desc",
    "service_info",
    "service_info_ref"
    ]
df_2022.columns = [
    "service_cat_name_high",
    "service_cat",
    "service_cat_name",
    "service_desc",
    "service_info",
    "service_info_changes"
    ]
df_2023.columns = [
    "service_cat_name_high",
    "service_cat",
    "service_cat_name",
    "service_desc",
    "service_info",
    "service_info_changes"
    ]

# Add year
df_2021["service_cat_year"] = 2021
df_2022["service_cat_year"] = 2022
df_2023["service_cat_year"] = 2023

# Combine all DFs
df = pd.concat([df_2021, df_2022, df_2023])

# Run in project root folder
path = "./src/osta/resources/service_codes.csv"
df.to_csv(path)
