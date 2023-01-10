
# Get indices of those rows that are changed
ind_mod = (org_uniq.fillna("") != org_uniq_mod.fillna("")).sum(axis=1) > 0
ind_mod = [i for i, x in enumerate(ind_mod) if x]


for i in ind_mod:
    # Get old and new rows
    old_row = org_uniq.iloc[i, :].values
    new_row = org_uniq_mod.iloc[i, :].values
    # Assign values based on bid, number or name whether they are found and not None
    if ("bid" in cols_to_match and old_row[cols_to_match.index("bid")] is not None):
        # Get which rows of df match the value
        col = cols_to_match.index("bid")
        ind = df_org.iloc[:, col] == old_row[col]
    elif ("code" in cols_to_match and old_row[cols_to_match.index("code")] is not None):
        # Get which rows of df match the value
        col = cols_to_match.index("code")
        ind = df_org.iloc[:, col] == old_row[col]
    else:
        # Get which rows of df match the value
        col = cols_to_match.index("name")
        ind = df_org.iloc[:, col] == old_row[col]
    # Replace values
    df_org.loc[ind, :] = new_row
        
    

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
