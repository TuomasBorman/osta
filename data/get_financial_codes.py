#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import tempfile
import requests
import zipfile
import re
import os


# Get location of temp folder
temp_dir = tempfile.TemporaryDirectory()

# Fetch directory from Avoindata.fi
url = ("https://www.avoindata.fi/data/dataset/fb33b030-" +
       "6d68-479c-bfd2-d3cb899a4376/resource/" +
       "2932f646-5c32-445b-a5f3-caa4b0a71b63/" +
       "download/taulukkomallit_30062020.zip")
r = requests.get(url)

# Split URL to get the file name
filename = url.split('/')[-1]
filename = temp_dir.name + "/" + filename
# Writing the file to temp folder
with open(filename, 'wb') as output_file:
    output_file.write(r.content)
# Extract folder
with zipfile.ZipFile(filename, 'r') as zip_ref:
    zip_ref.extractall(temp_dir.name)

# Get all the files as list
filename = re.sub(".zip", "", filename)
files = os.listdir(filename)

# Get the labels of values
file = filename + "/" + [x for x in files if re.search("KKTR", x)][0]
sheets = pd.ExcelFile(file).sheet_names
sheets = [x for x in sheets if x not in ["Sisällys", "t00", "Setup"]]
df_lab = pd.DataFrame()
for i in sheets:
    fields = pd.read_excel(file, sheet_name=i)
    fields = fields.iloc[4:, [1, 3]]
    fields = fields.dropna(how="any")
    fields.columns = ["label", "code"]
    fields["entity"] = "KKTR"
    df_lab = pd.concat([df_lab, fields], axis=0, ignore_index=True)
# Get the labels of values
file = filename + "/" + [x for x in files if re.search("KKNR", x)][0]
sheets = pd.ExcelFile(file).sheet_names
sheets = [x for x in sheets if x not in ["Sisällys", "t00", "Setup"]]
for i in sheets:
    fields = pd.read_excel(file, sheet_name=i)
    fields = fields.iloc[:, 1:]
    fields = fields.dropna(how="any")
    # Get only correct columns
    temp1 = fields.loc[:, fields.astype(str).apply(
        lambda x: all(x.str.isnumeric()))]
    fields = pd.concat([fields.iloc[:, 0], temp1], axis=1)
    fields = pd.melt(fields, id_vars=[fields.columns[0]], value_name="code")
    fields = fields.rename(columns={"Unnamed: 1": "label"})
    fields["entity"] = "KKNR"
    df_lab = pd.concat([df_lab, fields], axis=0, ignore_index=True)
# Get the labels of values
file = filename + "/" + [x for x in files if re.search("KKOTR", x)][0]
sheets = pd.ExcelFile(file).sheet_names
sheets = [x for x in sheets if x not in ["Sisällys", "t00", "Setup"]]
for i in sheets:
    fields = pd.read_excel(file, sheet_name=i)
    fields = fields.iloc[4:, [1, 3]]
    fields = fields.dropna(how="any")
    fields.columns = ["label", "code"]
    fields["entity"] = "KKOTR"
    df_lab = pd.concat([df_lab, fields], axis=0, ignore_index=True)
df_lab = df_lab.loc[:, ["label", "code", "entity"]]
df_lab = df_lab.loc[df_lab["code"].astype(str).str.isdigit(), ]
df_lab = df_lab.reset_index(drop=True)
# Add file to directory
path = "../src/osta/resources/financial_codes.csv"
df_lab.to_csv(path)
# Remove temp folder
temp_dir.cleanup()


# REMOVE UNNECESSARY FILES