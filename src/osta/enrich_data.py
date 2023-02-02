#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import osta.__utils as utils
import pandas as pd
import warnings
import pkg_resources
import requests
import sys
import selenium.webdriver as webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as firefox_opt
from selenium.webdriver.chrome.options import Options as chrome_opt
from selenium.webdriver.ie.options import Options as ie_opt
import re


def enrich_data(df, **args):
    """
    Change column names of pandas.DataFrame

    Arguments:
        ```
        df: pandas.DataFrame containing invoice data.

        ```

    Details:

    Examples:
        ```

        ```

    Output:


    """
    # INPUT CHECK
    # df must be pandas DataFrame
    if not utils.__is_non_empty_df(df):
        raise Exception(
            "'df' must be non-empty pandas.DataFrame."
            )
    # INPUT CHECK END

    # Add organization data
    df = __add_org_data(df, **args)
    # Add supplier data
    df = __add_suppl_data(df, **args)
    # Add account data
    df = __add_account_data(df, **args)
    # Add service data
    df = __add_service_data(df, **args)
    # Add missing total, vat_amount or price_ex_vat
    df = __add_sums(df, **args)
    return df


def __add_org_data(df, disable_org=False, org_data=None, **args):
    """
    This function adds organization data to dataset.
    Input: df (and dataset to be added)
    Output: enriched df
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
    # Load default database
    if org_data is None:
        # path = pkg_resources.resource_filename(
        #     "osta", "resources/" + "municipality_codes.csv")
        path = "~/Python/osta/src/osta/resources/" + "municipality_codes.csv"
        org_data = pd.read_csv(path, index_col=0)
    # Column of db that are matched with columns that are being checked
    # Subset to match with cols_to_check
    cols_to_match = ["bid", "vat_number", "number", "name"]
    cols_to_match = [cols_to_match[i] for i, x in enumerate(cols_df)
                     if x in cols_to_check]
    # Add data from database
    df = __add_data_from_db(df=df, df_db=org_data,
                            cols_to_check=cols_to_check,
                            cols_to_match=cols_to_match,
                            prefix="org")
    return df


def __add_account_data(df, disable_account=False, account_data=None,
                       subset_account_data=None, **args):
    """
    This function adds account data to dataset.
    Input: df (and dataset to be added)
    Output: enriched df
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
    # Check if column(s) is found as non-duplicated
    cols_df = ["account_number", "account_name"]
    cols_to_check = utils.__not_duplicated_columns_found(df, cols_df)
    if disable_account or len(cols_to_check) == 0:
        return df
    # INPUT CHECK END
    # Load default database
    if account_data is None:
        # path = pkg_resources.resource_filename(
        #     "osta", "resources/" + "account_info.csv")
        path = "~/Python/osta/src/osta/resources/" + "account_info.csv"
        account_data = pd.read_csv(path, index_col=0)
        # Subset by taking only specific years
        account_data = __subset_data_based_on_year(df, df_db=account_data,
                                                   **args)
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
    # Add data from database
    df = __add_data_from_db(df=df, df_db=account_data,
                            cols_to_check=cols_to_check,
                            cols_to_match=cols_to_match,
                            prefix="account")
    return df


def __add_service_data(df, disable_service=False, service_data=None, **args):
    """
    This function adds service data to dataset.
    Input: df (and dataset to be added)
    Output: enriched df
    """
    # INPUT CHECK
    if not (utils.__is_non_empty_df(service_data) or service_data is None):
        raise Exception(
            "'org_data' must be non-empty pandas.DataFrame or None."
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
    # Load default database
    if service_data is None:
        # path = pkg_resources.resource_filename(
        #     "osta", "resources/" + "service_codes.csv")
        path = "~/Python/osta/src/osta/resources/" + "service_codes.csv"
        service_data = pd.read_csv(path, index_col=0)
        # Subset by taking only specific years SIIRRÄ UTILSIIN
        service_data = __subset_data_based_on_year(df, df_db=service_data,
                                                   **args)
    # Column of db that are matched with columns that are being checked
    # Subset to match with cols_to_check
    cols_to_match = ["number", "name"]
    cols_to_match = [cols_to_match[i] for i, x in enumerate(cols_df)
                     if x in cols_to_check]
    # Add data from database
    df = __add_data_from_db(df=df, df_db=service_data,
                            cols_to_check=cols_to_check,
                            cols_to_match=cols_to_match,
                            prefix="service")
    return df


def __add_suppl_data(df, disable_suppl=False, suppl_data=None, **args):
    """
    This function adds supplier data to dataset.
    Input: df and dataset to be added
    Output: enriched df
    """
    # INPUT CHECK
    if not (utils.__is_non_empty_df(suppl_data) or suppl_data is None):
        raise Exception(
            "'org_data' must be non-empty pandas.DataFrame or None."
            )
    if not isinstance(disable_suppl, bool):
        raise Exception(
            "'disable_suppl' must be True or False."
            )
    # Check if column(s) is found as non-duplicated
    cols_df = ["suppl_id", "suppl_vat_number", "suppl_number", "suppl_name"]
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
        # Add data from database
        df = __add_data_from_db(df=df, df_db=suppl_data,
                                cols_to_check=cols_to_check,
                                cols_to_match=cols_to_match,
                                prefix="suppl")
    return df


def __add_data_from_db(df, df_db, cols_to_check, cols_to_match, prefix):
    """
    This function is a general function for adding data from a file
    to dataset.
    Input: df and dataset to be added
    Output: enriched df
    """
    # Which column are found from df and df_db SIIRRÄ UTILSIIN
    cols_df = [x for x in cols_to_check if x in df.columns]
    cols_df_db = [x for x in cols_to_match if x in df_db.columns]
    # Drop those columns that do not have match in other df
    if len(cols_df) > len(cols_df_db):
        cols_to_check = [cols_df[cols_to_match.index(x)] for x in cols_df_db]
        cols_to_match = cols_df_db
    else:
        cols_to_match = [cols_df_db[cols_to_check.index(x)] for x in cols_df]
        cols_to_check = cols_df
    # If identification coluns were not found
    if len(cols_to_check) == 0 or len(cols_to_match) == 0:
        warnings.warn(
            message=f"'{prefix}_data' should include at least one of the "
            "following columns: 'name' (name), 'number' "
            "(number), and 'bid' (business ID for organization and "
            f"supplier data).",
            category=Warning
            )
        return df
    # Get columns that will be added to data/that are not yet included
    cols_to_add = [x for x in df_db.columns if x not in cols_to_match]
    # Get only the first variable
    col_to_check = cols_to_check[0]
    col_to_match = cols_to_match[0]
    # If there are columns to add
    if len(cols_to_add) > 0:
        # Remove duplicates if database contains multiple values
        # for certain information.
        df_db = df_db.drop_duplicates(subset=col_to_match)
        # Create temporary columns which are used to merge data
        temp_x = df.loc[:, col_to_check]
        temp_y = df_db.loc[:, col_to_match]
        # Subset database and add prefix tp column names
        df_db = df_db.loc[:, cols_to_add]
        df_db.columns = prefix + "_" + df_db.columns
        # If variables can be converted into numeric, do so.
        # Otherwise convert to object if datatypes are not equal
        if (all(temp_x.dropna().astype(str).str.isnumeric()) and all(
                temp_y.dropna().astype(str).str.isnumeric())):
            temp_x = pd.to_numeric(temp_x)
            temp_y = pd.to_numeric(temp_y)
        elif temp_x.dtype != temp_y.dtype:
            temp_x = temp_x.astype(str)
            temp_y = temp_y.astype(str)
        # Add temporary columns to data
        df.loc[:, "temporary_X"] = temp_x
        df_db.loc[:, "temporary_Y"] = temp_y
        # Merge data
        df = pd.merge(df, df_db, how="left",
                      left_on="temporary_X", right_on="temporary_Y")
        # Remove temproary columns
        df = df.drop(["temporary_X", "temporary_Y"], axis=1)
    return df


def __add_sums(df, disable_sums=False):
    """
    This function adds sums (total, vat_amount or price_ex_vat) if
    some is missing.
    Input: df
    Output: enriched df
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
    col_missing = [x for x in cols_df if x not in cols_to_check]

    # If there were only one column missing, calculate them
    if len(col_missing) == 1 and all(
            x in ["float64", "int64"] for x in df.loc[
                :, cols_to_check].dtypes):
        # If total is missing
        if "total" in col_missing:
            df["total"] = df["price_ex_vat"] + df["vat_amount"]
        # If price_ex_vat is missing
        elif "price_ex_vat" in col_missing:
            df["price_ex_vat"] = df["total"] - df["vat_amount"]
        # If vat_amount is missing
        elif "vat_amount" in col_missing:
            df["vat_amount"] = df["total"] - df["price_ex_vat"]
    return df


def fetch_company_data(ser, language="en", only_ltd=False, **args):
    """
    Fetch company data from databases.

    Arguments:
        ```
        ser: pd.Series including business IDs.

        language: A string specifying the language of fetched data. Must be
        "en" (English), "fi" (Finnish), or "sv" (Swedish).

        only_ltd: A Boolean value specifying whether to search results also
        for other than limited companies. The search for them is slower.
        (By default: only_ltd=False)

        ```

    Details:
        This function fetches company data from Finnish Patent and Registration
        Office (Patentti- ja Rekisterihallitus, PRH) and The Business
        Information System (Yritystietojärjestelmä, YTJ). Resources of
        services are limited. Please use the function only when needed, and
        store the results if possible. Search in smaller batches to prevent
        problems with resource allocation.

    Examples:
        ```
        bids = pd.Series(["1458359-3", "2403929-2"])
        df = fetch_company_data(bids)
        ```

    Output:
        df with company data
    """
    # INPUT CHECK
    if not (isinstance(ser, pd.Series) and len(ser) > 0):
        raise Exception(
            "'ser' must be non-empty pandas.Series."
            )
    if not (isinstance(language, str) and language in ["fi", "en", "sv"]):
        raise Exception(
            "'language' must be 'en', 'fi', or 'sv'."
            )
    if not isinstance(only_ltd, bool):
        raise Exception(
            "'only_ltd' must be True or False."
            )
    # INPUT CHECK END
    # Get language in right format for database
    lan = "se" if language == "sv" else language
    # Remove None values and duplicates
    ser = ser.dropna()
    ser = ser.drop_duplicates()
    # For progress bar, specify the width of it
    progress_bar_width = 50
    # Initialize resulst DF
    df = pd.DataFrame()
    # Loop though BIDs
    for bid_i, bid in enumerate(ser.to_numpy()):
        # update the progress bar
        percent = 100*(bid_i/(len(ser)-1))
        sys.stdout.write('\r')
        sys.stdout.write("Completed: [{:{}}] {:>3}%"
                         .format('='*int(percent/(100/progress_bar_width)),
                                 progress_bar_width, int(percent)))
        sys.stdout.flush()
        # Get data from database
        path = "https://avoindata.prh.fi/bis/v1/" + str(bid)
        r = requests.get(path)
        # Convert to dictionaries
        text = r.json()
        # Get results only
        df_temp = pd.json_normalize(text["results"])
        # If results were found, continue
        if not df_temp.empty:
            # Change names
            df_temp = df_temp.rename(columns={
                "businessId": "bid",
                "name": "name",
                "registrationDate": "registration_date",
                "companyForm": "company_form_short",
                "liquidations": "liquidation",
                "companyForms": "company_form",
                "businessLines": "business_line",
                "registedOffices": "muni",
                "businessIdChanges": "old_bid",
                })
            # Get certain data and convert into Series
            col_info = ["bid", "name"]
            series = df_temp.loc[:, col_info]
            series = series.squeeze()
            # Loop over certain information columns
            info = [
                    "liquidation",
                    "company_form",
                    "business_line",
                    "muni",
                    "old_bid",
                    ]
            for col in info:
                # Get data
                temp = df_temp[col]
                temp = temp.explode().apply(pd.Series)
                # If information is included
                if len(temp.dropna(axis=0, how="all")) > 0:
                    if any(x in col for x in ["company_form",
                                              "business_line",
                                              "muni"]):
                        # If certain data, capitalize and add column names
                        # with language
                        # Remove those values that are outdated
                        ind = temp["endDate"].isna()
                        if any(ind):
                            temp = temp.loc[ind, :]
                        # Get only specific language
                        ind = temp["language"].astype(str).str.lower() == lan
                        if any(ind):
                            temp_name = temp.loc[ind, "name"].astype(
                                str).str.capitalize()
                        else:
                            temp_name = temp.loc[:, "name"].astype(
                                str).str.capitalize()
                        # Ensure that there is only one value
                        temp_name = temp_name.iloc[[0]]
                        temp_name.index = [col]
                    elif any(x in col for x in ["liquidation"]):
                        # If certain data, get name and date with
                        # specific language
                        ind = temp["language"].astype(str).str.lower() == lan
                        if any(ind):
                            temp_name = temp.loc[ind, "description"].astype(
                                str).str.capitalize()
                            temp_date = temp.loc[ind, "registrationDate"]
                        else:
                            temp_name = temp.loc[:, "description"].astype(
                                str).str.capitalize()
                            temp_date = temp.loc[:, "registrationDate"]
                        # Ensure that there is only one value
                        temp_name = temp_name.iloc[[0]]
                        temp_date = temp_date.iloc[[0]]
                        # Add names
                        temp_name.index = [col]
                        temp_date.index = [col + "_date"]
                        # Combine results
                        temp_name = pd.concat([temp_name, temp_date])
                    elif any(x in col for x in ["old_bid"]):
                        # If certain data, capitalize and add
                        # column names with numbers
                        temp_name = temp["oldBusinessId"]
                        temp_col = [col]
                        if len(temp_name) > 1:
                            temp_col.extend([col + "_" + str(x) for x in
                                             range(2, len(temp_name)+1)])
                        temp_name.index = temp_col
                    # Add to final data
                    series = pd.concat([series, temp_name])
            # Convert Series to DF and transpose it to correct format
            res = pd.DataFrame(series).transpose()
        elif not only_ltd:
            # If BID was not found from the database, try to find
            # with web search
            try:
                res = __fetch_company_data_from_website(bid, language)
            except Exception:
                res = pd.DataFrame([bid], index=["bid"]).transpose()
        else:
            # If user want only ltd info and data was not found
            res = pd.DataFrame([bid], index=["bid"]).transpose()
        # Add to DataFrame
        if df.empty:
            df = res
        else:
            df = pd.merge(df, res, how="outer")
    # Combine BID columns into one
    if "old_bid" in df.columns:
        regex = re.compile(r"old_bid")
        ind = [True if regex.search(x) else False for x in df.columns]
        bid_cols = df.loc[:, ind]
        bid_col = bid_cols.apply(lambda x: ', '.join(x.dropna(
            ).astype(str)), axis=1)
        # Remove additional BID columns, keep only one
        ind = [False if regex.search(x) else True for x in df.columns]
        df = df.loc[:, ind]
        df["old_bid"] = bid_col
    # Stop progress bar
    sys.stdout.write("\n")
    return df


def __fetch_company_data_from_website(bid, language):
    """
    This function fetch company data from PRH's website that includes
    all companies (not just limited company).
    Input: business ID or business name
    Output: df with company data
    """
    # Test if BID is business ID or name
    # bid_option = utils.__are_valid_bids(pd.Series([bid])).all()
    bid_option = True
    # Create a driver
    driver_found = False
    for driver in ["firefox", "chrome", "ie"]:
        # Try to use if these browsers are available
        try:
            if driver == "firefox":
                options = firefox_opt()
                options.add_argument("--headless")
                browser = webdriver.Firefox(options=options)
            elif driver == "chrome":
                options = chrome_opt()
                options.add_argument("--headless")
                browser = webdriver.Chrome(options=options)
            elif driver == "ie":
                options = ie_opt()
                options.add_argument("--headless")
                browser = webdriver.Ie(options=options)
        except Exception:
            pass
        else:
            driver_found = True
            break
    # IF driver was found
    if driver_found:
        # Set implicit wait time
        browser.implicitly_wait(5)
        # Get urö based on language
        if language == "fin":
            url = "https://tietopalvelu.ytj.fi/yrityshaku.aspx?kielikoodi=1"
        elif language == "sv":
            url = "https://tietopalvelu.ytj.fi/yrityshaku.aspx?kielikoodi=2"
        else:
            url = "https://tietopalvelu.ytj.fi/yrityshaku.aspx?kielikoodi=3"
        # Get results
        res = __search_companies_with_web_search(bid, bid_option,
                                                 url, browser)
    else:
        # If driver was not found
        res = [bid, None] if bid_option else [None, bid]
        res.extend([None for x in range(1, 13)])
    # Names of fields
    colnames = [
        "bid",
        "name",
        "company_form",
        "muni",
        "business_line",
        "liquidation",
        ]
    # Create series
    df = pd.DataFrame(res, index=colnames).transpose()
    # Remove Nones
    df = df.dropna(axis=1)
    return df


def __search_companies_with_web_search(bid, bid_option, url, browser):
    """
    Help function, this function fetch company data from PRH's website. Search
    is similar for different languages
    Input: business ID, url and driver
    Output: list including company data
    """
    # Go to the web page
    browser.get(url)
    # Search by BID or name
    if bid_option:
        search_box = browser.find_element(
            "xpath", "//input[@id='_ctl0_cphSisalto_ytunnus']")
    else:
        search_box = browser.find_element(
            "xpath", "//input[@id='_ctl0_cphSisalto_hakusana']")
    search_box.send_keys(bid)
    # Submit the text to search bar
    search_box.send_keys(Keys.RETURN)
    # Find the link for result web page
    link = browser.find_elements(
        "xpath", "//a[@id='_ctl0_cphSisalto_rptHakuTulos__ctl1_HyperLink1']")
    link = link[0].get_attribute("href") if len(link) == 1 else None
    # Go to the result web page
    if link is not None:
        # BeautifulSoup could be used but it misses the language information
        browser.get(link)
        bid = browser.find_elements(
            "xpath", "//span[@id='_ctl0_cphSisalto_lblytunnus']")
        bid = bid[0].text if len(bid) == 1 else None
        # Find name
        name = browser.find_elements(
            "xpath", "//span[@id='_ctl0_cphSisalto_lblToiminimi']")
        name = name[0].text if len(name) == 1 else None
        # Find company form
        company_form = browser.find_elements(
            "xpath", "//span[@id='_ctl0_cphSisalto_lblYritysmuoto']")
        company_form = company_form[0].text if len(company_form) == 1 else None
        # Find home town
        registed_office = browser.find_elements(
            "xpath", "//span[@id='_ctl0_cphSisalto_lblYrityksenKotipaikka']")
        registed_office = registed_office[
            0].text.capitalize() if len(registed_office) == 1 else None
        # Find business line
        business_line = browser.find_elements(
            "xpath", "//span[@id='_ctl0_cphSisalto_lblYrityksenToimiala']")
        business_line = business_line[
            0].text if len(business_line) == 1 else None
        # Find liquidation information
        liquidation = browser.find_elements(
            "xpath", "//span[@id='_ctl0_cphSisalto_lblKonkurssitieto']")
        liquidation = liquidation[0].text if len(liquidation) == 1 else None
        # Combine result
        res = [bid, name, company_form, registed_office,
               business_line, liquidation]
    else:
        # Give list with Nones, if link to result page is not found
        res = [bid, None] if bid_option else [None, bid]
        res.extend([None for x in range(1, 5)])
    return res


def fetch_org_data(org_codes, years=None, language="en"):
    """
    Fetch municipality data from databases.

    Arguments:
        ```
        org_codes: pd.Series including municipality codes.

        years: None or pd.Series including years specifying the year of data
        that will be fetched. If not None, the lenght must be equal with
        'org_codes'. If None, the most previous data will be fetched.
        (By default: years=None)

        language: A string specifying the language of fetched data. Must be
        "en" (English), "fi" (Finnish), or "sv" (Swedish).
        ```

    Details:
        This function fetches municipality key figures from the database of
        Statistics Finland (Tilastokeskus).

    Examples:
        ```
        codes = pd.Series(["005", "020"])
        years = pd.Series(["02.05.2021", "20.10.2020"])
        df = fetch_org_data(codes, years, language="fi")
        ```

    Output:
        pd.DataFrame including municipality data.
    """
    # INPUT CHECK
    if not (isinstance(org_codes, pd.Series) and len(org_codes) > 0):
        raise Exception(
            "'org_codes' must be non-empty pandas.Series."
            )
    if not ((isinstance(years, pd.Series) and len(years) == len(org_codes))
            or years is None):
        raise Exception(
            "'years' must be None or non-empty pandas.Series matching with " +
            "'org_codes'."
            )
    if not (isinstance(language, str) and language in ["fi", "en", "sv"]):
        raise Exception(
            "'language' must be 'en', 'fi', or 'sv'."
            )
    # INPUT CHECK END
    # If years were provided
    if years is not None:
        try:
            # Test if year can be detected
            years = pd.to_datetime(years).dt.year
            years = years.astype(str)
        except Exception:
            warnings.warn(
                message="'years' were not detected. The most recent data " +
                "from database is being fetched.",
                category=Warning
                )
            years = None
    # Find the most recent data
    url = "https://statfin.stat.fi/PXWeb/api/v1/fi/Kuntien_avainluvut"
    r = requests.get(url)
    text = r.json()
    available_years = [x.get("id") for x in text]
    year_max = max(available_years)
    if years is None:
        years = [year_max for x in range(0, len(org_codes))]
    # Check which years are in time series database
    url = ("https://statfin.stat.fi/PXWeb/api/v1/fi/Kuntien_avainluvut/" +
           year_max)
    r = requests.get(url)
    try:
        text = r.json()
        # Find available years based on pattern in id
        found_year = [x.get("text") for x in text if x.get("id") ==
                      "kuntien_avainluvut_" + year_max + "_aikasarja.px"][0]
        p = re.compile("\\d\\d\\d\\d-\\d\\d\\d\\d")
        found_year = p.search(found_year).group().split("-")
        # Getn only years that are available
        years_temp = [x if int(x) in
                      range(int(found_year[0]), int(found_year[1]))
                      else None for x in years]
        years_not_found = [x for i, x in enumerate(years_temp)
                           if x != years[i]]
        if len(years_not_found):
            warnings.warn(
                message=f"The following 'years' were not found from the "
                f"database: {years_not_found}",
                category=Warning
                )
    except Exception:
        years_temp = years
    # Check which municipalties are found from the database / are correct
    # path = pkg_resources.resource_filename(
    #     "osta", "resources/" + "municipality_codes.csv")
    path = "~/Python/osta/src/osta/resources/" + "municipality_codes.csv"
    org_data = pd.read_csv(path, index_col=0, dtype="object")
    # Get only correct municipality codes
    codes_temp = [x if x in org_data["number"].tolist() else
                  None for x in org_codes]
    codes_not_found = [x for i, x in enumerate(codes_temp)
                       if x != org_codes[i]]
    if len(codes_not_found):
        warnings.warn(
            message=f"The following 'codes' were not found from the database: "
            f"{codes_not_found}",
            category=Warning
            )
    # Create DF, drop duplicates, and remove incorrect years and codes
    df = pd.DataFrame({"code": codes_temp, "year": years_temp})
    df = df.drop_duplicates()
    df = df.dropna()
    # Get URL and correct parameters of the time series database
    url = ("https://pxdata.stat.fi:443/PxWeb/api/v1/" + language +
           "/Kuntien_avainluvut/" + year_max + "/kuntien_avainluvut_" +
           year_max + "_aikasarja.px")
    params = {"query": [{"code": "Alue " + year_max,
                         "selection": {"filter": "item", "values":
                                       df["code"].drop_duplicates().tolist()}},
                        {"code": "Vuosi",
                         "selection": {"filter": "item", "values":
                                       df["year"
                                          ].drop_duplicates().tolist()}}],
              "response": {"format": "json-stat2"}
              }
    # Find results
    r = requests.post(url, json=params)
    if r.status_code:
        text = r.json()
        # Find labels, code, years and values
        label = list(text.get("dimension").get("Tiedot").get(
            "category").get("label").values())
        code = list(text.get("dimension").get("Alue 2021").get(
            "category").get("label").keys())
        years = list(text.get("dimension").get("Vuosi").get(
            "category").get("label").values())
        values = text.get("value")
        # Divide values based on mucipalities
        values_num = int(len(values)/len(code))
        df_temp = pd.DataFrame()
        for i in range(0, len(values), values_num):
            # Split based on organization
            temp = pd.Series(values[i:i+values_num])
            # Split based on year
            temp = [temp[i::len(years)].tolist() for i in range(len(years))]
            temp = pd.DataFrame(temp).transpose()
            df_temp = pd.concat([df_temp, temp], axis=1)
        # Add label and code
        df_temp.index = label
        df_temp.loc["code", :] = [x for x in code for i in range(len(years))]
        year_temp = []
        for x in [years for i in range(len(code))]:
            year_temp.extend(x)
        df_temp.loc["year", :] = year_temp
        df_temp = df_temp.transpose()
        df = pd.merge(df, df_temp)
    return df
