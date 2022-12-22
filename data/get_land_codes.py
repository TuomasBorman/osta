# -*- coding: utf-8 -*-

import requests
import pandas as pd


query = "https://fi.wikipedia.org/wiki/Luettelo_ISO_3166_-standardin_koodeista"
page = requests.get(query)
df = pd.read_html(page.text)[0]
df.to_csv("land_codes.csv")
