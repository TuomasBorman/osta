#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import osta.__utils as utils
import pandas as pd
import warnings
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pkg_resources
import numpy as np


def clean_data(df, **args):
    """
    Standardize data

    This function standardize the data that is in pandas.DataFrame. The
    function expects that columns are named in specific way. If the data
    does not meet requirements, the function gives a warning.

    Arguments:
        `df`: pandas.DataFrame containing invoice data.

        `**args`: Additional arguments passes into other functions:

        `date_format`: The format of date that will be in output data.
        (By default: date_format="%d-%m-%Y")

        `dayfirst`: Boolean value or None to specify the format of date
        in input data. 'If day comes before month in the date.'
        If None, function determines the format automatically.
        (By default: dayfirst=None)

        `yearfirst`: Boolean value or None to specify the format of date
        in input data. 'If year comes first in the date.'
        If None, function determines the format automatically.
        (By default: yearfirst=None)

        `country_format`: A string speciying the format of country in the
        output data. Must be one of the following options: 'name_fin',
        'name_en', "code_2char", 'code_3char', 'code_num', or 'code_iso'.
        (By default: country_format='code_2char')

        `pattern_th`: A numeric value [0,1] specifying the threshold of
        enough good match between names, e.g., 'org_name', and database.
        Value over threshold have enough strong pattern and it is
        interpreted to be a match. (By default: pattern_th=0.7)

        `scorer`: A scorer function passed into fuzzywuzzy.process.extractOne
        function that evaluates the match between names and database.
        (By default: scorer=fuzz.token_sort_ratio)

        `org_data`: pd.DataFrame or None. Database for organization data.
        Data must include on of the following columns: 'name', 'number', or
        'bid' specifying the name, number and business ID of organizations,
        respectively. If None, packages default database is used.
        (By default: org_data=None)

        `suppl_data`: pd.DataFrame or None. Database for supplier data.
        Data must include on of the following columns: 'name', 'number', or
        'bid' specifying the name, number and business ID of suppliers,
        respectively. If None, no database is used.
        (By default: org_data=None)

        `account_data`: pd.DataFrame or None. Database for account data.
        Data must include on of the following columns: 'name', or
        'number' specifying the name and number accounts,
        respectively. If None, packages default database is used.
        (By default: account_data=None)

        `subset_account_data`: None or string to specify which database is
        used for account data. Available options are 'balance_sheet' and
        'income_statement'. If None, both databases are used and the result
        value will be the first occurence.
        (By default: subset_account_data=None)

        `service_data`: pd.DataFrame or None. Database for service data.
        Data must include on of the following columns: 'name', or
        'number' specifying the name and number service categories,
        respectively. If None, packages default database is used.
        (By default: service_data=None)

        `db_year`: Integer value or None specifying the year (2021, 2021,
        or 2023) that will be used to match account and service data.
        If None, the most current information is used and duplicates from
        previous years are removed. (By default: db_year=None)

        `disable_*`: A boolean value specifying whether * data
        is checked. * can be one of the following options: 'org',
        'suppl', 'date', 'sums', 'country', 'voucher', 'account' or
        'service'. (By default: disable_*=False)

    Details:
        This function standardize the data and checks that it is in correct
        format. If the data is not in expected format containing erroneous
        data that cannot be standardized, the function gives a warning.
        Moreover, empty rows and columns along with spaces before or after
        values are removed.

        The function expects that columns are in specific format.
        Not all the columns must be included in the data. Below is a
        list of columns and what type of data they should have.
            -org_name: Organization name
            -org_number: Organization number
            -org_id: Organization business ID
            -account_name: The name of account to where invoice is allocated.
            -account_number: The number of account to where incoice is
            allocated.
            -service_cat_name: The name of service category to where invoice
            is allocated.
            -service_cat: The number of service category to where invoice is
            allocated.
            -suppl_name: Supplier name
            -supplier number: Supplier number
            -suppl_id: Supplier business ID
            -vat_number: VAT number of supplier
            -country: Country of supplier.
            -voucher: Running identification of invoice
            -date: Date of invoice.
            -price_ex_vat: Price excluding VAT.
            -vat_amount: Amount of VAT.
            -total: Price indluding VAT.

        Organization data (name, number, BID) is matched with database.
        Uncorrect values are replaced with correct values unless the
        specified organization matches with multiple organizations in the
        database. The user can also use own database.

        Supplier data (name, number, BID, VAT number) is not matched with
        database by default and replacements are not done. However, user
        can use own database. Like in case of organization data, the function
        checks that BID and VAT numbers are in correct format and that they
        duplicated meaning that each value specifies only one supplier.

        Account and service information. Account and service information is
        matched with database, and uncorrect values are replaced unless they
        match with multiple accounts / service categories. For account data,
        user can specify if balance sheet or income statement database is used.
        Otherwise, both are used and first occurence will be the result value.
        The user can also use own database.

        Country is identified with database containing all common land codes.
        (Finnish and English names, 2 and 3-character codes, along with
        numeric and ISO code). User can define the format of output from
        formats included in the database.

        Date is automatically identified unless user do not define the format.
        Dates are converted into format that user can specify.

        Price excluding and including VAT and VAT amount (abbreviated as sums
        in the function) are checked by calculating if values of them match
        between each other. If all three columns are not available, the
        the function only converts values in float type if possible.

    Examples:
        ```
        # Create a dummy data
        data = {"org_name": ["Turun kaupunki", "Turku", "Turku"],
                "org_number": [None, None, None],
                "date": ["02.01.2023", "2-1-2023", "1.1.2023"],
                "suppl_name": ["Myyjä", "Supplier Oy", "Myyjän tuote Oy"],
                "country": ["FI", "FI", "FI"],
                "total": [100.21, 10.30, 50.50],
                }
        df = pd.DataFrame(data)
        df = clean_data(df, country_format="name_fin")
        ```

    Output:
        pandas.DataFrame with standardized data.

    """
    # INPUT CHECK
    # df must be pandas DataFrame
    if not utils.__is_non_empty_df(df):
        raise Exception(
            "'df' must be non-empty pandas.DataFrame."
            )
    # INPUT CHECK END
    # Check if there are empty rows or columns, and remove them
    if any(df.isna().all(axis=0)) or any(df.isna().all(axis=1)):
        df = df.dropna(axis=0, how="all")
        df = df.dropna(axis=1, how="all")
        warnings.warn(
            message="'df' contained empty rows or/and columns \n" +
            "that are now removed.\n",
            category=Warning
            )
    # Remove spaces from beginning and end of the value
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Test if voucher is correct
    __check_voucher(df, **args)
    # Check sums
    df = __clean_sums(df, **args)
    # Check dates
    df = __standardize_date(df, **args)
    # Check org information:
    df = __standardize_org(df, **args)
    # Check supplier info
    df = __standardize_suppl(df, **args)
    # Check account info
    df = __standardize_account(df, **args)
    # Check service data
    df = __standardize_service(df, **args)
    # Check country data
    df = __standardize_country(df, **args)
    return df


def __standardize_country(df, disable_country=False,
                          country_format="code_2char", **args):
    """
    This function check standardize the used country format.
    Input: df
    Output: df
    """
    # INPUT CHECK
    if not isinstance(disable_country, bool):
        raise Exception(
            "'disable_country' must be True or False."
            )
    # Check if column(s) is found as non-duplicated
    cols_to_check = ["country"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_to_check)
    if disable_country or len(cols_to_check) == 0:
        return df
    # INPUT CHECK END
    # Load data base
    path = pkg_resources.resource_filename(
        "osta", "resources/" + "land_codes.csv")
    country_codes = pd.read_csv(path, index_col=0)
    # country_format must be one of the database columns
    if not (isinstance(country_format, str) and
            country_format in country_codes.columns):
        raise Exception(
            f"'country_format' must be one of the following options\n."
            f"{country_codes.columns.tolist()}"
            )
    # INPUT CHECK END
    # Get country data from df
    df_country = df[cols_to_check]
    # Get unique countries
    df_country = df_country.drop_duplicates()
    # Loop over values and add info on them
    not_found = pd.DataFrame()
    for i, x in df_country.iterrows():
        # Get the country from data base; case insensitive search
        ind = country_codes.applymap(
            lambda col: str(col).lower()).isin(
                x.astype(str).str.lower().values.tolist()).sum(axis=1) > 0
        if any(ind):
            temp = country_codes.loc[ind, :]
            # Assing result to original DF
            df.loc[df[cols_to_check[0]] == x[0], cols_to_check[0]] = temp[
                country_format].values[0]
        else:
            not_found = pd.concat([not_found, x], axis=1)
    # If some countries were not detected
    if not_found.shape[1] > 0:
        warnings.warn(
            message=f"The following countries were not detected. Please check "
            f"them for errors: \n{not_found}",
            category=Warning
            )
    return df


def __clean_sums(df, disable_sums=False, **args):
    """
    This function checks that sums (total, vat, netsum) are in float format,
    and tries to convert them if they are not. Futhermore, if one field is
    missing, it is calculated based on others.
    Input: df
    Output: df
    """
    # INPUT CHECK
    if not isinstance(disable_sums, bool):
        raise Exception(
            "'disable_sums' must be True or False."
            )
    # Check if column(s) is found as non-duplicated
    cols_df = ["total", "vat_amount", "price_ex_vat"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_df)
    if disable_sums or len(cols_to_check) == 0:
        return df
    # INPUT CHECK END
    # Get columns that are missing from the data
    cols_missing = [x for x in cols_df if x not in cols_to_check]

    # If data is not float, try to make it as float
    if len(cols_to_check) > 0 and not all(df.dtypes[
            cols_to_check] == "float64"):
        # Get those column names that need to be modified
        cols_not_float = df.dtypes[cols_to_check][
            df.dtypes[cols_to_check] != "float64"].index
        # Loop throug columns
        for col in cols_not_float:
            # Replace "," with "." and remove spaces
            df[col] = df[col].astype(str).str.replace(",", ".")
            # Try to convert values as float
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                warnings.warn(
                    message=f"The following column cannot be converted into "
                    f"float: {col}",
                    category=Warning
                    )
    # Calcute the expected value, and check if it's matching
    if len(cols_missing) == 0 and all(df.dtypes[cols_to_check] == "float64"):
        test = df["price_ex_vat"] + df["vat_amount"]
        # Get columns that do not match
        temp = df.loc[df["total"] != test, cols_to_check]
        # If any unmatching was found
        if temp.shape[0] > 0:
            temp = temp.drop_duplicates()
            warnings.warn(
                message=f"The sums of following rows do not match. Please "
                f"check them for errors: \n{temp}",
                category=Warning
                )
    # If datatype is not correct
    if not all(df.dtypes[cols_to_check] == "float64"):
        warnings.warn(
            message=f"The following columns are not float type. Please "
            f"check them for errors: \n{cols_to_check}",
            category=Warning
            )
    return df


def __standardize_date(df, disable_date=False, date_format="%d-%m-%Y",
                       dayfirst=None, yearfirst=None, **args):
    """
    This function identifies the format of dates and standardize them.
    Input: df
    Output: df
    """
    # INPUT CHECK
    if not isinstance(disable_date, bool):
        raise Exception(
            "'disable_date' must be True or False."
            )
    # Check if column(s) is found as non-duplicated
    cols_to_check = ["date"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_to_check)
    if disable_date or len(cols_to_check) == 0:
        return df
    col_to_check = cols_to_check[0]
    # dayfirst and yearfirst must be None or boolean value
    if not (isinstance(dayfirst, bool) or dayfirst is None):
        raise Exception(
            "'dayfirst' must be True or False or None."
            )
    if not (isinstance(yearfirst, bool) or yearfirst is None):
        raise Exception(
            "'yearfirst' must be True or False or None."
            )
    # INPUT CHECK END
    convert_date = False
    # Get date column
    df_date = df.loc[:, col_to_check]
    # Split dates from separator. Result is multiple columns
    # with year, month and day separated
    df_date_mod = df_date.astype(str).str.split(r"[-/.]", expand=True)
    # If the split was succesful
    if df_date_mod.shape[1] > 1:
        # Get format of dates if None
        if not (isinstance(dayfirst, bool) or isinstance(yearfirst, bool)):
            df_date_mod, dayfirst, yearfirst = __get_format_of_dates_w_sep(
                df_date_mod)
        convert_date = True
    # Try to reformat DDMMYYYY format
    elif utils.__test_if_date(df_date_mod.iloc[:, 0]):
        # Get only the series
        df_date_mod = df_date_mod.iloc[:, 0]
        # Get format of date
        char_len = int(max(df_date_mod.astype(str).str.len()))
        # Get format of dates if None
        if dayfirst is None or yearfirst is None:
            dayfirst, yearfirst = __get_format_of_dates_wo_sep(df_date_mod)
        # If format was found, standardize
        if dayfirst is not None and yearfirst is not None:
            # Add separators to dates
            df_date_mod = __convert_dates_without_sep(
                df_date_mod,
                char_len=char_len,
                dayfirst=dayfirst,
                yearfirst=yearfirst)
            convert_date = True
    # Convert dates
    if convert_date and dayfirst is not None and yearfirst is not None:
        # Standardize dates
        df[col_to_check] = pd.to_datetime(df_date_mod, dayfirst=dayfirst,
                                          yearfirst=yearfirst,
                                          errors="coerce")
        # Change the formatting
        df[col_to_check] = df[col_to_check].dt.strftime(date_format)
        # Check if there were Nones
        df_date = df_date[df.loc[:, col_to_check].isna()].values.tolist()
        if len(df_date) > 0:
            warnings.warn(
                message=f"The format of following dates where not detected, "
                f"and they are converted to NaN. "
                f"Please check them for errors: {df_date}",
                category=Warning
                )
    else:
        warnings.warn(
            message="The format of dates where not detected, "
            "and the 'date' column is unchanged. Please check that dates "
            "have separators between days, months, and years.",
            category=Warning
            )
    return df


def __get_format_of_dates_w_sep(df):
    """
    This function identifies the format of dates that are with
    separators between days, months and years. Furthermore, date is modified so
    that year has 4 characters and separators between days, months and years
    are uniformed.
    Input: df with days months and years in own columns
    Output: Modified df, if year comes first, if day comes first
    """
    # Disable day convert by default
    dayfirst = None
    yearfirst = None
    df_date = None
    # Check if columns can be converted into int. If not, do not
    if not all([all(df[x].astype(str).str.isdigit()) for x in df.columns]):
        return [df_date, dayfirst, yearfirst]
    # Get the number of characters in each column If there are some column with
    # 4 characters, year is in longer format
    year_len = 4 if any(
        np.vectorize(len)(df.values.astype(str)).max(axis=0) == 4) else 2
    # Get values which should match
    if year_len == 4:
        year = list(range(1970, 2050))
    else:
        year = list(range(15, 50))
    month = list(range(1, 13))
    day = list(range(1, 32))

    # Loop over columns. Are the values matching with days, months or years?
    res = pd.DataFrame()
    for i, x in enumerate(df.columns):
        # Get value
        temp_val = df.iloc[:, i].astype(int)
        # Get matches
        is_day = (max(day) >= temp_val.max() and temp_val.min() >= min(day))
        is_month = (max(month) >= temp_val.max()
                    and temp_val.min() >= min(month))
        is_year = (max(year) >= temp_val.max() and temp_val.min() >= min(year))
        # Add to DF
        temp = pd.DataFrame([is_day, is_month, is_year])
        temp.index = ["is_day", "is_month", "is_year"]
        res = pd.concat([res, temp], axis=1)
    # Results to columns, rows are original dates separated from separator
    res = res.transpose()

    # If year has only 2characters, take first days. Otherwise, take years
    if year_len == 2:
        day_res = (res["is_day"].values &
                   ~res["is_month"].values & ~res["is_year"].values)
        year_res = ~res["is_month"].values & res["is_year"].values
    else:
        year_res = (~res["is_day"].values &
                    ~res["is_month"].values & res["is_year"].values)
        day_res = (res["is_day"].values & ~res["is_month"].values)

    # Get indices of matches; if this date columns has days or years?
    day_res = day_res.nonzero()[0]
    year_res = year_res.nonzero()[0]
    # If only one match per day/year was found, get the final date format
    if len(day_res) == 1 and len(year_res) == 1:
        yearfirst = True if year_res == 0 else False
        if yearfirst:
            # Get format
            dayfirst = True if day_res == 1 else False
            # Get values
            dd = (df.iloc[:, 1] if dayfirst else df.iloc[:, 2])
            dd = dd.str.zfill(2)
            mm = (df.iloc[:, 2] if dayfirst else df.iloc[:, 1])
            mm = mm.str.zfill(2)
            yyyy = (df.iloc[:, 0] if year_len == 4 else
                    "20" + df.iloc[:, 0].str.zfill(2))
            # Create a date in YYYY/XX/XX format
            df_date = ((yyyy + "/" + dd + "/" + mm) if
                       dayfirst else (yyyy + "/" + mm + "/" + dd))
        else:
            # Get format
            dayfirst = True if day_res == 0 else False
            # Get values
            dd = (df.iloc[:, 0] if dayfirst else df.iloc[:, 1])
            dd = dd.str.zfill(2)
            mm = (df.iloc[:, 1] if dayfirst else df.iloc[:, 0])
            mm = mm.str.zfill(2)
            yyyy = (df.iloc[:, 2] if year_len == 4 else
                    "20" + df.iloc[:, 2].str.zfill(2))
            # Create a date in YYYY/XX/XX format
            df_date = ((dd + "/" + mm + "/" + yyyy) if
                       dayfirst else (mm + "/" + dd + "/" + yyyy))
    df_date = pd.Series(df_date)
    return [df_date, dayfirst, yearfirst]


def __get_format_of_dates_wo_sep(df):
    """
    This function identifies the format of dates that are without
    separators between days, months and years.
    Input: series
    Output: If year comes first, if day comes first
    """
    # Get maximum number of characters
    char_len = int(max(df.astype(str).str.len()))
    yearfirst = None
    dayfirst = None
    if char_len == 8 or char_len == 6:
        # Expected year range
        years = list(range(1970, 2050)) if char_len == 8 else list(
            range(15, 50))
        # Get only those values that have maximum number of characters
        ind = df.astype(str).str.len() == char_len
        date_temp = df[ind]

        # Find place of year
        year_len = 4 if char_len == 8 else 2
        i = 0
        j = year_len
        res_year = pd.DataFrame()
        for x in range(0, char_len-1, year_len):
            i_temp = i + x
            j_temp = j + x
            temp = date_temp.astype(str).str[i_temp:j_temp]
            # Which values are between expected years?
            res = max(years) >= int(max(temp)) >= min(years)
            # Add to DF
            res = pd.DataFrame([i_temp, j_temp, res], index=["i", "j", "res"])
            res_year = pd.concat([res_year, res], axis=1)
        # Adjust column names
        res_year.columns = range(res_year.shape[1])
        # Get values based on results
        i_year = [res_year.loc["i", x] for i, x in enumerate(res_year.columns)
                  if res_year.loc["res", x]]
        j_year = [res_year.loc["j", x] for i, x in enumerate(res_year.columns)
                  if res_year.loc["res", x]]
        # Get only the individual values, if there are only one valid result
        if (len(i_year) == 1 and len(j_year) == 1):
            i_year_ind = int(i_year[0])
            j_year_ind = int(j_year[0])
            # Remove year from dates
            if i_year_ind == 0:
                date_temp = date_temp.astype(str).str[j_year_ind:]
            else:
                date_temp = date_temp.astype(str).str[:i_year_ind]
            # Get place of the year
            yearfirst = True if i_year_ind == 0 else False
    # If year was found
    if yearfirst is not None:
        # Expected day and month ranges
        months = list(range(1, 13))
        days = list(range(1, 32))
        # Ger place of the month and day
        i = 0
        j = 2
        res_day = pd.DataFrame()
        for x in range(0, char_len-year_len-1, 2):
            i_temp = i + x
            j_temp = j + x
            temp = date_temp.astype(str).str[i_temp:j_temp]
            # Which values are between expected days?
            res_d = max(days) >= int(max(temp)) >= min(days)
            # Which values are between expected months?
            res_m = max(months) >= int(max(temp)) >= min(months)
            # Add to DF
            res = pd.DataFrame([res_d, res_m], index=["day", "month"])
            res_day = pd.concat([res_day, res], axis=1)
        # Adjust column names
        res_day.columns = range(res_day.shape[1])
        # Get index of where month is located
        month_i = [i for i, x in enumerate(res_day.columns)
                   if res_day.loc["day", x] and res_day.loc["month", x]]
        if len(month_i) == 1:
            month_ind = int(month_i[0])
            # If month was the latter
            dayfirst = True if month_ind == res_day.shape[1]-1 else False
    # Combine result
    result = [dayfirst, yearfirst]
    return result


def __convert_dates_without_sep(df, char_len, dayfirst, yearfirst):
    """
    This function standardizes the dates that are without
    separators between days, months and years.
    Input: series
    Output: list of values with separators
    """
    # Create result DF and add columns
    res = pd.DataFrame(df)
    res.columns = ["original"]
    res = res.assign(mod=res["original"])
    res = res.assign(day=None)
    res = res.assign(month=None)
    res = res.assign(year=None)

    # Modify first values that have maximum amount of characters
    year_len = 4 if char_len == 8 else 2
    # Get the year based on yearfirst, and remove year from dates
    if yearfirst:
        year = res["mod"].astype(str).str[:year_len]
        res["mod"] = res["mod"].astype(str).str[year_len:]
    else:
        year = res["mod"].astype(str).str[-year_len:]
        res["mod"] = res["mod"].astype(str).str[:-year_len]
    # Add year to results
    res["year"] = year

    # Add days and months for sure cases (2 or 1 characters for both days
    # and months)
    for i in [1, 2]:
        dm_len = char_len-year_len if i == 2 else char_len-year_len-2
        ind = res["mod"].astype(str).str.len() == dm_len
        # Based on dayfirst, get values; remove them from mod column
        if dayfirst:
            day = res.loc[ind, "mod"].astype(str).str[:i]
            month = res.loc[ind, "mod"].astype(str).str[i:]
        else:
            month = res.loc[ind, "mod"].astype(str).str[:i]
            day = res.loc[ind, "mod"].astype(str).str[i:]
        # Add to data
        res.loc[ind, "day"] = day
        res.loc[ind, "month"] = month
        # Remove from mod column
        res.loc[ind, "mod"] = ""

    # For values that have 3 character, we have to do differently, because
    # we cannot be sure what values are months and what day.
    # Determine this by looking a common pattern
    # Expected day and month ranges
    if any(res["mod"] != ""):
        months = list(range(1, 13))
        days = list(range(1, 32))
        # Get indices where day+month has 3 characters
        ind = res["mod"].astype(str).str.len() == 3

        # Get tests ranges; in which range 1st and 2nd set of values should be?
        temp_test1 = days if dayfirst else months
        temp_test2 = months if dayfirst else days
        res_day = pd.DataFrame()
        # Loop over number of characters that values can have
        for i in [1, 2]:
            # Get values
            temp1 = res["mod"].astype(str).str[:i]
            temp2 = res["mod"].astype(str).str[i:]
            # Test values
            temp1 = max(temp_test1) >= int(max(temp1)) >= min(temp_test1)
            temp2 = max(temp_test2) >= int(max(temp2)) >= min(temp_test2)
            # Add to DF
            len1 = i
            len2 = max(res["mod"].astype(str).str.len())-i
            temp = pd.DataFrame([len1, len2, temp1, temp2])
            res_day = pd.concat([res_day, temp], axis=1)
        # Adjust index and column names
        index = ["day", "month"] if dayfirst else ["month", "day"]
        res_day.index = ["len1", "len2"] + index
        res_day.columns = range(res_day.shape[1])
        # Get values where the pattern is correct; how many characters
        # days and months have?
        res_i = res_day.loc[index, :].sum(axis=0) == res_day.loc[
            index, :].shape[0]
        if sum(res_i) == 1:
            res_day = res_day.loc[["len1", "len2"], res_i]
            # Get place of the month and day, based on dayfirst
            if dayfirst:
                day = res.loc[ind, "mod"].astype(str).str[
                    :res_day.loc["len1"].values[0]]
                month = res.loc[ind, "mod"].astype(str).str[
                    res_day.loc["len1"].values[0]:]
            else:
                month = res.loc[ind, "mod"].astype(str).str[
                    :res_day.loc["len1"].values[0]]
                day = res.loc[ind, "mod"].astype(str).str[
                    res_day.loc["len1"].values[0]:]
            # Add to data
            res.loc[ind, "day"] = day
            res.loc[ind, "month"] = month
            # Remove from mod column
            res.loc[ind, "mod"] = ""

    # Combine result to date with separators
    res = res["day"] + "/" + res["month"] + "/" + res["year"]
    return res


def __standardize_org(df, disable_org=False, org_data=None, **args):
    """
    This function prepares organization data to be checked, and calls
    function that checks it.
    Input: df
    Output: df with standardized organization data
    """
    # INPUT CHECK
    if not (utils.__is_non_empty_df(org_data) or org_data is None):
        raise Exception(
            "'org_data' must be non-empty pandas.DataFrame or None."
            )
    if not isinstance(disable_org, bool):
        raise Exception(
            "'disable_org' must be True or False."
            )
    # Check if column(s) is found as non-duplicated
    cols_df = ["org_id", "org_vat_number", "org_number", "org_name"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_df)
    if disable_org or len(cols_to_check) == 0:
        return df
    # INPUT CHECK END
    if org_data is None:
        path = pkg_resources.resource_filename(
            "osta", "resources/" + "municipality_codes.csv")
        org_data = pd.read_csv(path, index_col=0, dtype=str)
    # Column of db that are matched with columns that are being checked
    # Subset to match with cols_to_check
    cols_to_match = ["bid", "vat_number", "number", "name"]
    cols_to_match = [cols_to_match[i] for i, x in enumerate(cols_df)
                     if x in cols_to_check]
    # Standardize organization data
    df = __standardize_based_on_db(df=df, df_db=org_data,
                                   cols_to_check=cols_to_check,
                                   cols_to_match=cols_to_match,
                                   **args)
    # If organization data was not in database, it is not checked.
    __check_org_data(df, cols_to_check)
    return df


def __standardize_account(df, disable_account=False, account_data=None,
                          subset_account_data=None, **args):
    """
    This function prepares account data to be checked, and calls
    function that checks it.
    Input: df
    Output: df with standardized organization data
    """
    # INPUT CHECK
    if not (utils.__is_non_empty_df(account_data) or account_data is None):
        raise Exception(
            "'account_data' must be non-empty pandas.DataFrame or None."
            )
    if not isinstance(disable_account, bool):
        raise Exception(
            "'disable_account' must be True or False."
            )
    if not (subset_account_data is None or
            subset_account_data == "balance_sheet" or
            subset_account_data == "income_statement"):
        raise Exception(
            "'subset_account_data' must be 'balance_sheet', "
            "'income_statement' or None."
            )
    # Check if column(s) is found as non-duplicated
    cols_df = ["account_number", "account_name"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_df)
    if disable_account or len(cols_to_check) == 0:
        return df
    # INPUT CHECK END
    if account_data is None:
        path = pkg_resources.resource_filename(
            "osta", "resources/" + "account_info.csv")
        account_data = pd.read_csv(path, index_col=0)
        # Subset by taking only specific years
        account_data = utils.__subset_data_based_on_year(
            df, df_db=account_data, **args)
        # If user specified balance sheet or income statement,
        # get only specified accounts
        if subset_account_data is not None:
            account_data = account_data.loc[
                :, account_data["cat_1"] == subset_account_data]
    # Column of db that are matched with columns that are being checked
    # Subset to match with cols_to_check
    cols_to_match = ["number", "name"]
    cols_to_match = [cols_to_match[i] for i, x in enumerate(cols_df)
                     if x in cols_to_check]
    # Data types to check
    dtypes = ["int64", "object"]
    # Standardize organization data
    df = __standardize_based_on_db(df=df, df_db=account_data,
                                   cols_to_check=cols_to_check,
                                   cols_to_match=cols_to_match,
                                   disable_partial=True, **args)
    # Check that values are matching
    __check_variable_pair(df, cols_to_check=cols_to_check, dtypes=dtypes)
    return df


def __standardize_service(df, disable_service=False,
                          service_data=None, **args):
    """
    This function prepares service data to be checked, and calls
    function that checks it.
    Input: df
    Output: df with standardized organization data
    """
    # INPUT CHECK
    if not (utils.__is_non_empty_df(service_data) or service_data is None):
        raise Exception(
            "'service_data' must be non-empty pandas.DataFrame or None."
            )
    if not isinstance(disable_service, bool):
        raise Exception(
            "'disable_service' must be True or False."
            )
    # Check if column(s) is found as non-duplicated
    cols_df = ["service_cat", "service_cat_name"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_df)
    if disable_service or len(cols_to_check) == 0:
        return df
    # INPUT CHECK END
    if service_data is None:
        path = path = pkg_resources.resource_filename(
            "osta", "resources/" + "service_codes.csv")
        service_data = pd.read_csv(path, index_col=0)
        # Subset by taking only specific years
        service_data = utils.__subset_data_based_on_year(
            df, df_db=service_data, **args)
    # Column of db that are matched with columns that are being checked
    # Subset to match with cols_to_check
    cols_to_match = ["number", "name"]
    cols_to_match = [cols_to_match[i] for i, x in enumerate(cols_df)
                     if x in cols_to_check]
    # Data types to check
    dtypes = ["int64", "object"]
    # Standardize organization data
    df = __standardize_based_on_db(df=df, df_db=service_data,
                                   cols_to_check=cols_to_check,
                                   cols_to_match=cols_to_match,
                                   disable_partial=True, **args)
    # Check that values are matching
    __check_variable_pair(df, cols_to_check=cols_to_check, dtypes=dtypes)
    return df


def __standardize_suppl(df, disable_suppl=False, suppl_data=None, **args):
    """
    This function prepares supplier data to be checked, and calls
    function that checks it.
    Input: df
    Output: df with standardized supplier data
    """
    # INPUT CHECK
    if not (utils.__is_non_empty_df(suppl_data) or suppl_data is None):
        raise Exception(
            "'suppl_data' must be non-empty pandas.DataFrame or None."
            )
    if not isinstance(disable_suppl, bool):
        raise Exception(
            "'disable_suppl' must be True or False."
            )
    # Check if column(s) is found as non-duplicated
    cols_df = ["suppl_id", "vat_number", "suppl_number", "suppl_name",
               "country"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_df)
    if disable_suppl or len(cols_to_check) == 0:
        return df
    # INPUT CHECK END
    if suppl_data is not None:
        # Column of db that are matched with columns that are being checked
        # Subset to match with cols_to_check
        cols_to_match = ["bid", "vat_number", "number", "name", "country"]
        cols_to_match = [cols_to_match[i] for i, x in enumerate(cols_df)
                         if x in cols_to_check]
        # Standardize organization data
        df = __standardize_based_on_db(df=df, df_db=suppl_data,
                                       cols_to_check=cols_to_check,
                                       cols_to_match=cols_to_match,
                                       **args)
    # Check that data is not duplicated, BID is correct, and there are not
    # empty values. Get warning if there are.
    # Check VAT numbers
    __check_vat_number(df, cols_to_check=["suppl_id", "vat_number", "country"],
                       **args)
    __check_org_data(df, cols_to_check)
    return df


def __standardize_based_on_db(df, df_db,
                              cols_to_check, cols_to_match,
                              pattern_th=0.7, scorer=fuzz.token_sort_ratio,
                              disable_partial=False,
                              **args):
    """
    Standardize the data based on database.
    Input: df, df_db including database,
    pattern_th to be used to match partial matching names
    Output: Standardized data
    """
    # INPUT CHECK
    # pattern_th must be numeric value 0-1
    if not utils.__is_percentage(pattern_th):
        raise Exception(
            "'pattern_th' must be a number between 0-1."
            )
    # Value [0,1] to a number between 0-100, because fuzzywuzzy requires that
    pattern_th = pattern_th*100
    # INPUT CHECK END
    # Which column are found from df and df_db
    cols_df = [x for x in cols_to_check if x in df.columns]
    cols_df_db = [x for x in cols_to_match if x in df_db.columns]
    # Drop those columns that do not have match in other df
    if len(cols_df) > len(cols_df_db):
        cols_to_check = [cols_df[cols_to_match.index(x)] for x in cols_df_db]
        cols_to_match = cols_df_db
    else:
        cols_to_match = [cols_df_db[cols_to_check.index(x)] for x in cols_df]
        cols_to_check = cols_df
    # If matching columns between df and data base were found
    if len(cols_to_check) > 0 and len(cols_to_match) > 0:
        # Subset the data
        df_org = df.loc[:, cols_to_check]
        # Drop duplicates from the database
        df_db = df_db.drop_duplicates(subset=cols_to_match)
        # Drop duplicates so we can focus on unique rows
        org_uniq = df_org.drop_duplicates()
        # Get matching values from database; replace incorrect values in
        # table including only unique values
        org_uniq_mod = __get_matches_from_db(df=org_uniq,
                                             df_db=df_db,
                                             cols_to_check=cols_to_check,
                                             cols_to_match=cols_to_match,
                                             pattern_th=pattern_th,
                                             disable_partial=disable_partial,
                                             scorer=scorer)
        # Replace values in original DataFrame columns
        df_org = __replace_old_values_with_new(df=df_org,
                                               old_values=org_uniq,
                                               new_values=org_uniq_mod,
                                               cols_to_check=cols_to_check,
                                               cols_to_match=cols_to_match)
        # Assign data back to original data, if some values were changed
        if any((df.loc[:, cols_to_check].fillna("") !=
                df_org.fillna("")).sum(axis=1) > 0):
            df.loc[:, cols_to_check] = df_org
    # If no matching columns were found from the data base
    elif len(cols_to_match) == 0:
        temp = cols_to_check[0].split("_")[0]
        warnings.warn(
            message=f"'{temp}' should include at least one of the "
            "following columns: 'name' (name), 'number' "
            "(number), and 'bid' (business ID for organization and "
            f"supplier data).",
            category=Warning
            )
    return df


def __check_org_data(df, cols_to_check):
    """
    Check if organization data is not duplicated or has empty values, or
    incorrect business IDs.
    Input: df, columns that contain organization information
    Output: which rows are incorrect? None if check were not made
    """
    # Which column are found from df
    cols_to_check = [x for x in cols_to_check if x in df.columns]
    # If any columns were found
    res = None
    if len(cols_to_check) > 0:
        # Subset the data
        df = df.loc[:, cols_to_check]
        # Drop duplicates:
        # now there should be only unique number, name, ID combinations
        df = df.drop_duplicates()
        # Are there None values
        res = df.isna().sum(axis=1) > 0
        # BIDs found?
        if any(i in cols_to_check for i in ["org_id", "suppl_id"]):
            # Get bid column
            col_to_check = [x for x in ["org_id", "suppl_id"]
                            if x in cols_to_check][0]
            # Get BIDs
            col = df[col_to_check]
            # Check that bids are valid; True if not valid
            valid = utils.__are_valid_bids(col).values
            # Are bids duplicated?
            duplicated = col.isin(col[col.duplicated()])
            # Update result
            res = res | valid | duplicated
        # Name found?
        if any(i in cols_to_check for i in ["org_name", "suppl_name"]):
            # Get column
            col_to_check = [x for x in ["org_name", "suppl_name"]
                            if x in cols_to_check][0]
            # Get names
            col = df[col_to_check]
            # Are names duplicated?
            duplicated = col.isin(col[col.duplicated()])
            # Update result
            res = res | duplicated
        # Number found?
        if any(i in cols_to_check for i in ["org_number", "suppl_number"]):
            # Get column
            col_to_check = [x for x in ["org_number", "suppl_number"]
                            if x in cols_to_check][0]
            # Get numbers
            col = df[col_to_check]
            # Are numbers duplicated?
            duplicated = col.isin(col[col.duplicated()])
            # Update result
            res = res | duplicated
        if any(res):
            # Get only incorrect values
            df = df.loc[res, :]
            warnings.warn(
                message=f"Following organization data contains duplicated "
                f"or empty values or business IDs are incorrect. "
                f"Please check them for errors: \n{df}",
                category=Warning
                )
    return res


def __replace_old_values_with_new(df,
                                  old_values, new_values,
                                  cols_to_check, cols_to_match):
    """
    Replace values of df with new_values based on corresponding old_values
    Input: df, current values of it, and new values that replace old values
    Output: df with new values
    """
    # Which values were modified?
    # Get indices of those rows that are changed
    ind_mod = (old_values.fillna("") != new_values.fillna("")).sum(axis=1) > 0
    ind_mod = [i for i, x in enumerate(ind_mod) if x]
    # Loop over those rows and replace the values of original data columns
    for i in ind_mod:
        # Get old and new rows
        old_row = old_values.iloc[i, :].values.tolist()
        new_row = new_values.iloc[i, :].values.tolist()
        # Replace values with new ones
        df.loc[(df == old_row).sum(axis=1) == df.shape[1], :] = new_row
    return df


def __get_matches_from_db(df, df_db,
                          cols_to_check, cols_to_match,
                          pattern_th, scorer,
                          disable_partial):
    """
    Is there some data missing or incorrect? Based on df_db, this function
    replaces values of df.
    Input: df, and data base
    Output: df with correct values
    """
    # Create a copy that will be modified
    df_mod = df.copy()
    # Initialize DF for warning messages
    missmatch_df = pd.DataFrame()
    not_detected_df = pd.DataFrame()
    part_match_df = pd.DataFrame()

    # Loop over rows
    for i, row in df.iterrows():
        # Intialize a DF for variables' position in data base
        temp = pd.DataFrame()
        # Loop over variables
        for j, x in enumerate(cols_to_check):
            # Get name, number and id if they are found from the df,
            # otherwise get False
            var_df = row[df.columns == cols_to_check[j]]
            var_df = var_df.astype(str).str.lower().values
            # Can name, number and BID be found from the df_db?
            # Get True/False list
            var_db = df_db.loc[:, cols_to_match[j]].astype(
                str).str.lower().values
            var_db = var_db == var_df
            # Add to DF
            temp[x] = var_db
        # Loop over variables
        found = False
        missmatch = False
        for j, x in enumerate(cols_to_check):
            # If 1st, 2nd... or nth variable was found from database
            if any(temp[x]):
                row_db = df_db.loc[temp[x].tolist(), cols_to_match]
                # row_db = row_db.values.tolist()[0]
                found = True
                # If there were other variables,
                # check if they match with data base
                # values acquired by jth variable
                if temp.shape[1] > 1:
                    missmatch_df, missmatch = __check_if_missmatch_db(
                       temp=temp, row_db=row_db, j=j, df_db=df_db,
                       cols_to_check=cols_to_check,
                       cols_to_match=cols_to_match,
                       missmatch_df=missmatch_df)
            # If match was found, break for loop; do not check other variables
            if found:
                break
        # If false, try partial match if values include names
        if found is False and "name" in cols_to_match:
            # Get name from df and database
            name_df = row[df.columns == cols_to_check[
                cols_to_match.index("name")]].values[0]
            name_db = df_db.loc[:, "name"]
            # Try partial match, get the most similar name
            name_part = process.extractOne(name_df, name_db, scorer=scorer)
            # If the matching score is over threshold
            if disable_partial is False and name_part[1] >= pattern_th:
                # Get only the name
                name_part = name_part[0]
                # Find row based on name with partial match
                row_db = (df_db.loc[df_db.loc[:, "name"] == name_part,
                                    cols_to_match])
                # Add row to final data
                df_mod.iloc[i, :] = row_db.values.tolist()[0]
                # Store info for warning message
                temp = pd.DataFrame([name_df, name_part],
                                    index=[cols_to_check[
                                        cols_to_match.index("name")],
                                        "found match"])
                part_match_df = pd.concat([part_match_df, temp], axis=1)
                found = True
        # If match was found add row to final data
        if found and missmatch is False:
            # There might be multiple matches, get first one.
            row_db = row_db.iloc[[0], :] if row_db.shape[0] > 1 else row_db
            # Assign the row only
            df_mod.iloc[i, :] = row_db.iloc[0, :]
        else:
            # Store data for warning message: data was not found
            row = pd.DataFrame(row)
            not_detected_df = pd.concat([not_detected_df, row], axis=1)
    # If some data had missmatch
    if missmatch_df.shape[0] > 0:
        missmatch_df = missmatch_df.drop_duplicates()
        warnings.warn(
            message=f"The following data "
            f"did not match. Please check it for errors: "
            f"\n{missmatch_df.transpose()}",
            category=Warning
            )
    # If some data was not detected
    if not_detected_df.shape[0] > 0:
        not_detected_df = not_detected_df.drop_duplicates()
        warnings.warn(
            message=f"The following data "
            f"was not detected. Please check it for errors: "
            f"\n{not_detected_df.transpose()}",
            category=Warning
            )
    # If partial match of name was used
    if part_match_df.shape[0] > 0:
        part_match_df = part_match_df.drop_duplicates()
        warnings.warn(
            message=f"The following organization names were detected "
            f"based on partial matching:"
            f"\n{part_match_df.transpose().drop_duplicates()}",
            category=Warning
            )
    return df_mod


def __check_if_missmatch_db(temp, row_db, j, df_db,
                            cols_to_check, cols_to_match, missmatch_df):
    """
    This function checks if there are missmatch between row values of df
    and database.
    Input: df with indices that specify the row being checked currently,
    database row, j is the index of variable being checked,
    whole database, cols being checked, cols being matched, DF containing
    missmatches.
    Output: DF containing missmatches, and boolean value indicating if
    missmatch was found.
    """
    # Initialize result
    missmatch = False
    # Take other columns than j
    temp = temp.loc[:, [x for x in cols_to_check
                        if x not in cols_to_check[j]]]
    temp = temp.loc[:, temp.sum(axis=0) > 0]
    # If other variables had also matches
    if temp.shape[1] > 0:
        # Loop over variables
        for k, c in enumerate(temp.columns):
            # Get variable from database
            temp_db = df_db.loc[
                temp[c].values, [
                    x for x in cols_to_match
                    if x not in cols_to_match[j]]
                ].values.tolist()[0]
            # Get variable that was in values that will be
            # added to the final data if everything's OK
            value = [row_db.values.tolist()[0][num] for num, x in
                     enumerate(cols_to_match) if x not in
                     cols_to_match[j]]
            # Check if they equal
            if value != temp_db:
                # Get values
                names1 = temp.columns.tolist()
                # Store data for warning message
                names2 = list("Found " + x for x in names1)
                names = [x for z in zip(names1, names2) for x in z]
                values = pd.DataFrame([value, temp_db], index=names)
                missmatch_df = pd.concat([missmatch_df, values],
                                         axis=1, ignore_index=True)
                # Missmatch was found
                missmatch = True
    return [missmatch_df, missmatch]


def __check_variable_pair(df, cols_to_check, dtypes, **args):
    """
    This function checks variable pair that their data type is correct.
    Input: df, columns being checked, and expected data types
    Output: df
    """
    # Subset so that only available columns are checked
    ind = [i for i, x in enumerate(cols_to_check) if x in df.columns]
    cols_to_check = list(cols_to_check[i] for i in ind)
    dtypes = list(dtypes[i] for i in ind)
    # Check if data types match
    ind = [i for i in ind if df[cols_to_check[i]].dtype != dtypes[i]]
    # If not, give warning
    if len(ind) > 0:
        # Subset to include only missmatches
        cols_to_check = list(cols_to_check[i] for i in ind)
        dtypes = list(dtypes[i] for i in ind)
        warnings.warn(
            message=f"The following data has incorrect data types."
            f"Data types of {cols_to_check} should be {dtypes}, respectively.",
            category=Warning
            )
    return df


def __check_vat_number(df, cols_to_check, disable_vat_number=False, **args):
    """
    This function checks that VAT numbers has correct patterns
    and match with business IDs.
    Input: df
    Output: df
    """
    # INPUT CHECK
    if not isinstance(disable_vat_number, bool):
        raise Exception(
            "'disable_vat_number' must be True or False."
            )
    # Check if column(s) is found as non-duplicated
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_to_check)
    # All columns must be present
    if len(cols_to_check) != 3 or disable_vat_number:
        return df
    # INPUT CHECK END
    # Get vat number and bid column names
    # (do it this way, if orgaization gets also vat_number)
    vat_number_col = [x for x in cols_to_check if x in ["vat_number"]][0]
    bid_col = [x for x in cols_to_check if x in ["suppl_id", "org_id"]][0]
    country_col = [x for x in cols_to_check if x in ["country"]][0]

    # Drop NA
    df = df.loc[:, cols_to_check]
    df = df.dropna(subset=vat_number_col)

    # Test iv valid VAT number
    res = -utils.__are_valid_vat_numbers(df[vat_number_col])

    # If BIDs are available
    if bid_col and country_col:
        # Get bids
        bids = df[bid_col]
        # Remove "-"
        bids = bids.astype(str).str.replace("-", "")

        # Get country codes from data base
        path = pkg_resources.resource_filename(
            "osta",
            "resources/" + "land_codes.csv")
        codes = pd.read_csv(path, index_col=0)

        # Get unique countries
        uniq_countries = df[country_col].drop_duplicates()
        for country in uniq_countries:
            # Get the country from data base and get 2 character code
            temp = codes.loc[(codes == country).sum(axis=1) == 1, "code_2char"]
            temp = temp.values
            temp = temp[0] if len(temp) == 1 else ""
            # Add to data
            df.loc[df[country_col] == temp, "2_char_code"] = temp
        # Add country code to bid
        bids = df["2_char_code"] + bids
        # Check that it is same as VAT number
        not_vat = df[vat_number_col] != bids
        res = res | not_vat
    # If there were some VAT numbers that were incorrect, give a warning
    if any(res):
        df = df.loc[res, :]
        warnings.warn(
            message=f"The following VAT numbers were incorrect. They did not "
            f"match the correct pattern or match BIDs. "
            f"Please check them for errors: {df}",
            category=Warning
            )
    return df


def __check_voucher(df, disable_voucher=False, **args):
    """
    This function checks if voucher column includes vouchers.
    Input: df
    Output: df
    """
    # INPUT CHECK
    if not isinstance(disable_voucher, bool):
        raise Exception(
            "'disable_voucher' must be True or False."
            )
    cols_to_check = ["voucher"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_to_check)
    # All columns must be present
    if disable_voucher or len(cols_to_check) == 0:
        return df
    col_to_check = cols_to_check[0]
    # INPUT CHECK END
    # CHeck with other columns
    res = utils.__test_if_voucher(
        df=df,
        col_i=df.columns.tolist().index(col_to_check),
        colnames=df.columns.tolist())
    # Voucher should match with other data or it should increase
    if not (res or df[col_to_check].is_monotonic_increasing):
        warnings.warn(
            message="It seems that 'voucher' column does not include " +
            "voucher values. Please check it for errors.",
            category=Warning
            )
    return df
