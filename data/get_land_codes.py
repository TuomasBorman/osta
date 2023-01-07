# -*- coding: utf-8 -*-

import requests
import pandas as pd

query = "https://fi.wikipedia.org/wiki/Luettelo_ISO_3166_-standardin_koodeista"
page = requests.get(query)
df = pd.read_html(page.text)[0]
# Rename
df.columns = [
    "name_fin",
    "name_en",
    "code_2char",
    "code_3char",
    "code_num",
    "code_iso",
    ]
# Run in project root folder
path = "./src/ostan/resources/land_codes.csv"
df.to_csv(path)
