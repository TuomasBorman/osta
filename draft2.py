#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 18:12:22 2023

@author: tvborm
"""

org_uniq_mod = org_uniq.copy()
df_msg = pd.DataFrame()
# Initialize lists/DF for results
not_detected = pd.DataFrame()
part_match = []
org_uniq_mod
# Loop over rows
for i, row in org_uniq.iterrows():
    # Get name, number and id if they are found, otherwise get False
    # laita kaikki pieniksi kirjaimiksi
    name = (row[cols_to_match.index("name")] if
            "name" in cols_to_match else False)
    number = (row[cols_to_match.index("code")] if
              "code" in cols_to_match else False)
    bid = (row[cols_to_match.index("bid")] if
          "bid" in cols_to_match else False)
    # Can name, number and BID be found from the list
    name_found = (name is not False and any(df_db.loc[:,"name"].astype(str).str.lower() == name.lower()))
    number_found = (number is not False and
                    str(number).replace('.', '', 1).isdigit() and
                    any(df_db.loc[:, "code"].astype(int) == int(number)))
    bid_found = (bid is not False and any(df_db.loc[:, "bid"] == bid))

    # If bid was found, it takes precedence
    if bid_found:
        # Take the row based on business id
        row_db = (df_db.loc[df_db.loc[:, "bid"].astype(str).str.lower()
                            == bid.lower(), cols_to_match])
        # If name or number do not match to row
        if ((name_found and name.lower() !=
            row_db.loc[:, "name"].astype(str).str.lower()) or
            (number_found and
             int(bid) != int(row_db["bid"]))):
            # Add data to DataFrame for warning message
            # Get rows
            temp = row.tolist()
            temp.extend(row_db.iloc[0,:].tolist())
            # Get column names
            temp_name = row.index.tolist()
            temp_name.extend(row_db.columns.to_list())
            # Add to DataFrame for warning message
            df_temp = pd.DataFrame(temp, index=temp_name)
            df_msg = pd.concat([df_msg, df_temp], ignore_index=True)
        else:
            # Add row to final data
            org_uniq_mod.iloc[i, :] = row_db
    # If name was found, it takes precedence
    elif name_found:
        # Take the row based on name
        row_db = (df_db.loc[df_db.loc[:, "name"].astype(str).str.lower()
                            == name.lower(), cols_to_match])
        # If number do not match to row (bid was not found)
        if number_found and int(number) != int(row_db["code"]):
            # Add data to DataFrame for warning message
            # Get rows
            temp = row.tolist()
            temp.extend(row_db.iloc[0,:].tolist())
            # Get column names
            temp_name = row.index.tolist()
            temp_name.extend(row_db.columns.to_list())
            # Add to DataFrame for warning message
            df_temp = pd.DataFrame(temp, index=temp_name)
            df_msg = pd.concat([df_msg, df_temp], ignore_index=True)
        else:
            # Add row to final data
            org_uniq_mod.iloc[i, :] = row_db
    # If number was found
    elif number_found:
        # Take the row based on number
        row_db = (df_db.loc[df_db.loc[:, "code"].astype(int) ==
                            int(number), cols_to_match])
        # Add row to final data (bid and name was not found)
        org_uniq_mod.iloc[i, :] = row_db
    # TEst partial matching to name if name was present
    elif name is not False or name is not None:
        # Try partial match, get the most similar name
        name_part = process.extractOne(name, df_db.loc[:, "name"],
                                       scorer=scorer)
        # Value [0,1] to a number between 0-100
        match_th = match_th*100
        # If the matching score is over threshold
        if name_part[1] >= 60:
            # Get only the name
            name_part = name_part[0]
            part_match.append([name, name_part])
            # Find row based on name with partial match
            row_db = (df_db.loc[df_db.loc[:, "name"] == name_part,
                                cols_to_match])
            # Assign both to df
            # Add row to final data
            org_uniq_mod.iloc[i, :] = row_db
        else:
            # Add name, number and bid to DataFrame for warning message
            not_detected = pd.concat([not_detected, row])