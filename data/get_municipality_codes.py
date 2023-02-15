# -*- coding: utf-8 -*-
import requests
import pandas as pd

query = "https://fi.wikipedia.org/wiki/Luettelo_Suomen_kunnista"
page = requests.get(query)
df = pd.read_html(page.text, decimal=",")[0]
# Rename
df.columns = [
    "logo",
    "number",
    "name",
    "city_or_mun",
    "language",
    "mun_center",
    "province",
    "subregion",
    "population",
    "area_total_km2",
    "area_land_km2",
    "population_density_km-2",
    ]

# Remove pattern (citation)
df = df.replace(r"\[.*?\]", "", regex=True)
# Add leading zeroes to code
df["number"] = df["number"].astype(str).str.rjust(3, "0")

# Load BIDs
path = "data/municipality_bids.csv"
bids = pd.read_csv(path, index_col=0)
# Add BIDs
df = df.merge(bids, on="name")

columns = [
    "number",
    "name",
    "bid",
    "city_or_mun",
    "language",
    "mun_center",
    "province",
    "subregion",
    ]

df = df.loc[:, columns]
# Run in data folder
path = "src/osta/resources/municipality_codes.csv"
df.to_csv(path)
