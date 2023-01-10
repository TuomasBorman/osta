#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan  8 14:06:55 2023

@author: tvborm
"""

import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

scorer=fuzz.token_sort_ratio

data = {"org_name": ["Merikarvia", "Tudjfjls", "Merikarvian kunta", "Turku", "Merikarvian kunta"],
        "org_number": [484, None, '85555553', 853, None],
        "org_id": ["0135202-4", "0135202-4", "0135202-4", "01345202-4", "94744"]
        }
df = pd.DataFrame(data)

data = {"org_name": ["Mevia", "Tudjfjls", "Tudjfjls", "Turku", "Merilaitila"],
        "org_number": [484, None, '85555553', 853, None],
        "org_id": ["0135202-4", "0135202-4", "0135202-4", "01365202-4", "94744"]
        }
df = pd.DataFrame(data)

col_to_check = ["org_number", "org_name", "org_id"]
# Get columns that are included in data
col_found = [x for x in col_to_check if x in df.columns]
# Get columns that are missing from the data
col_missing = set(col_to_check).difference(col_found)
# Subset the data
df_org = df.loc[:, col_found]
# Drop duplicates, there should be as many rows as there are organizations
org_uniq = df_org.drop_duplicates()

# Load municipality data base if organizsatipon data is not provided!!
# ADD ALSO Other data on organization
# Y-tunnus take precedence, then number, then name
# SAme function can be used fro supplier?
# standardize_org - standardize_suppl - standardize_org_or_suppl
# tiedosto voidaan antaa myös tiedostopolkuna
path = "~/Python/osta/src/osta/resources/municipality_codes.csv"
mun_data = pd.read_csv(path, index_col=0)

# Remove leading and trailing spaces
df_org["org_name"].astype(str).str.strip()

# Check if all organization codes can be found from the list
if not all(org_uniq.loc[
        :, "org_number"].dropna().isin(mun_data.loc[:, "code"])):
    print("Ei löydy kaikkia")

# Check that all organization numbers match with names
names_from_db = []
for number in org_uniq.loc[:, "org_number"]:
    # Can it be found?
    if str(number).replace('.', '', 1).isdigit() and\
        any(int(number) ==
            mun_data.loc[:, "code"].astype(int)):
        temp = mun_data.loc[mun_data["code"].astype(int) ==
                            int(number), "name"].values[0]
    else:
        temp = None
    names_from_db.append(temp)
# Which names were found?
match = list(c == names_from_db[i] for i, c in enumerate(org_uniq.loc[:, "org_name"]))
org_uniq.loc[match, :]
org_uniq.loc[[not i for i in match], :]


match_th=0.6
import warnings
org_uniq_temp = org_uniq.copy()
not_detected = []
part_match = []
for row in org_uniq.index:
    # Get name, number and id
    name = org_uniq.loc[row, "org_name"]
    number = org_uniq.loc[row, "org_number"]
    bid = org_uniq.loc[row, "org_id"]
    # Can number be found from the list
    number_found = str(number).replace('.', '', 1).isdigit() and\
        any(mun_data.loc[:, "code"].astype(int) == int(number))
    # Can name be found from the list
    name_found = any(mun_data.loc[:, "name"] == name)
    # If number was not found, but name was
    if not number_found and name_found:
        number = mun_data.loc[name == mun_data.loc[:, "name"], "code"]
        # Assign value to df
        org_uniq_temp.loc[row, "org_number"] = number.values[0]
    # If name was not found, but number was
    elif not name_found and number_found:
        name = mun_data.loc[number == mun_data.loc[:, "code"], "name"]
        # Assign value to df
        org_uniq_temp.loc[row, "org_name"] = name.values[0]
    # If both were not found, try partial match
    elif not name_found and not number_found and (name.strip() and
                                                  name is not None):
        # Try partial match, get the most similar name
        name_part = process.extractOne(name, mun_data.loc[:, "name"],
                                       scorer=scorer)
        # Value [0,1] to a number between 0-100
        match_th = match_th*100
        # If the matching score is over threshold
        if name_part[1] >= 60:
            # Get only the name
            name_part = name_part[0]
            part_match.append([name, name_part])
            # Find number based on name
            number = mun_data.loc[mun_data.loc[:, "name"] == name_part, "code"]
            # Assign both to df
            org_uniq_temp.loc[row, "org_name"] = name_part
            org_uniq_temp.loc[row, "org_number"] = number.values[0]
        else:
            not_detected.append([number, name])
if len(not_detected) > 0:
    warnings.warn(
        message=f"The following organization number and name "
        f"pairs were not detected. Please check them for errors: "
        f"{not_detected}",
        category=Warning
        )
if len(part_match) > 0:
    warnings.warn(
        message=f"The following organization names were detected based on"
        f"partial matching: {part_match}",
        category=Warning
        )
org_uniq_temp

# Mitkä arvot ovat muuttuneet?
org_uniq != org_uniq_temp

org_mod = org_uniq.loc[(org_uniq.fillna("") != org_uniq_temp.fillna("")).sum(axis=1) > 0, :]

for row in org_mod.index:
    name = org_uniq.loc[row, "org_name"]
    # Get name, number and id
    old_name = org_uniq.loc[row, "org_name"]
    new_name = org_uniq_temp.loc[row, "org_name"]
    old_number = org_uniq.loc[row, "org_number"]
    new_number = org_uniq_temp.loc[row, "org_number"]
    bid = org_uniq.loc[row, "org_id"]
    # If name is not NaN, find matches based on it
    if old_name is not None:
        # Find matches
        ind = df_org.loc[:, "org_name"] == old_name
        # Replace name and number
        df_org.loc[ind, "org_name"] = new_name
        df_org.loc[ind, "org_number"] = new_number
    # Otherwise find matches based on number
    elif old_name is not None:
        # Replace values with new name
        ind = df_org.loc[:, "org_number"] == old_number
        df_org.loc[ind, "org_name"] = new_name
        df_org.loc[ind, "org_number"] = new_number
df_org

# Entä jos sama nimi mutta esim numero eroaa? Mikä on oikein, jos ei voida määrittää?
org_uniq = df_org.drop_duplicates()
org_uniq

# Find organization values that have mismatch, i.e.,
# number might differ when name is the same
df_dupl = pd.DataFrame()
for col in col_found:
    # Check if there are duplicated values
    is_dupl = org_uniq.duplicated(col)
    if any(is_dupl):
        # Get duplicated values
        temp = org_uniq.loc[is_dupl, col].drop_duplicates()
        # Get whole rows
        temp = org_uniq.loc[org_uniq[col] == temp.values[0], :]
        # Add rows to result data frame
        df_dupl = pd.concat([df_dupl, temp])
# Remove duplicates
df_dupl = df_dupl.drop_duplicates()
df_dupl
# If there are mismatching values, give warning. Without external data
# we cannot make decisions if one data is correct and other not.
if df_dupl.shape[0] > 0:
    warnings.warn(
        message=f"The following organization data mismatches, i.e., certain "
        f"value is linked to multiple organizations. The data is kept "
        f"unchanged. Please check it for errors: \n{df_dupl} ",
        category=Warning
        )

org_uniq.shape[0] != len(org_uniq["org_number"].drop_duplicates()) or org_uniq.shape[0] != len(org_uniq["org_name"].drop_duplicates())


number = org_uniq.loc[org_uniq.duplicated("org_number"), "org_number"].drop_duplicates()
name = org_uniq.loc[org_uniq.duplicated("org_name"), "org_name"].drop_duplicates()
bid = org_uniq.loc[org_uniq.duplicated("org_id"), "org_id"].drop_duplicates()

df_number = org_uniq.loc[org_uniq["org_number"] == number.values[0], :]
df_name = org_uniq.loc[org_uniq["org_name"] == name.values[0], :]
df_bid = org_uniq.loc[org_uniq["org_id"] == bid.values[0], :]
df_dupl = pd.concat([df_number, df_name, df_bid]).drop_duplicates()
if len(number) > 0 or len(name) > 0 or len(bid) > 0:
    
len(org_uniq["org_name"].drop_duplicates())
# If column is not empty
if name.strip():
    # Try partial match, get the most similar name
    name_part = process.extractOne(name, mun_data.loc[:, "name"], scorer=scorer)
    # Value [0,1] to a number between 0-100
    match_th = match_th*100
    # If the matching score is over threshold
    if name_part[1] >= match_th:
        # Get only the name
        name = name_part[0]

org_uniq.shape[0]
len(org_uniq.loc[:, "org_name"].drop_duplicates())
len(org_uniq.loc[:, "org_number"].drop_duplicates())
len(org_uniq.loc[:, "org_id"].drop_duplicates())

mun_data.loc[mun_data["code"] == org_uniq.loc[0,"org_number"], "name"]
# Check that org_name is correct, org_number is correct, org_id is correct
# Check that there are no NAs
# If there are, fill blanks and assign back
# ORganization was not detected!