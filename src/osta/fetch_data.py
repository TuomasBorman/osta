#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import warnings
import sys
import re
import codecs
import tempfile
import urllib.request
from zipfile import ZipFile
import shutil
import os
import filetype
import cchardet
import csv
from osta.change_names import change_names


def fetch_data_urls(search_words, **args):
    """
    Fetch the url addresses of downloadable datasets of Avoindata.fi.

    Arguments:
        `search_words`: A string specifying search words used to search
        datasets from the database.

    Details:
        This function fetches url addresses.

    Examples:
        ```
        # Search all purchase data
        df = fetch_data_urls("ostolasku")
        # Search only purchase data of Turku
        df = fetch_data_urls("ostolasku Turku")
        ```

    Output:
        pandas.DataFrame with url addresses.

    """
    # INPUT CHECK
    if not isinstance(search_words, str):
        raise Exception(
            "'search_words' must be a string."
            )
    # INPUT CHECK END
    df = pd.DataFrame()
    # Get query for searching pages that contain data
    url = "https://www.avoindata.fi"
    query = url + "/data/fi/" + "dataset?q=" + search_words
    # Search
    r = requests.get(query)
    # If the search was succesful
    if r.ok:
        # Parse dataset and find pages / individual search results
        soup = BeautifulSoup(r.content, "html.parser")
        pages = soup.find_all("div", {"class": "dataset-item"})
        # Find how many pages there are in total in the search results
        page_nums = soup.find_all("ul", {"class": "pagination"})
        if len(page_nums) > 0:
            page_nums = page_nums[0]
            page_nums = page_nums.findAll("a")
            pattern = re.compile(r"page=")
            page_nums = [int(x.get("href").split("page=")[1])
                         for x in page_nums if
                         bool(pattern.findall(x.get("href")))]
            max_page = max(page_nums)
        else:
            max_page = 1
        # Find how many results there are in total
        tot_num_res = soup.find(
            "h3", {"class": "m-0 search-form__result-text"}).text
        tot_num_res = int("".join(re.findall(r'[0-9]+', tot_num_res)))
        i = 1
        # For progress bar, specify the width of it
        progress_bar_width = 50
        for num in range(1, max_page+1):
            # If not the first page (which is already loaded), load the page
            if num != 1:
                query = (url + "/data/fi/" + "dataset?q=" +
                         search_words + "&page=" + str(num))
                # Search
                r = requests.get(query)
                # Parse dataset and find pages / individual search results
                soup = BeautifulSoup(r.content, "html.parser")
                pages = soup.find_all("div", {"class": "dataset-item"})
            # Loop through individual search results in the page
            for page in pages:
                # Update the progress bar
                percent = 100*((i)/tot_num_res)
                i += 1
                sys.stdout.write('\r')
                sys.stdout.write("Completed: [{:{}}] {:>3}%"
                                 .format('='*int(
                                     percent/(100/progress_bar_width)),
                                         progress_bar_width, int(percent)))
                sys.stdout.flush()
                # Get url to the search result page
                title_element = page.find(
                    "h3", class_="dataset-heading dataset-title")
                page_url = title_element.find(
                    "a", href=True)['href']
                # Create a new query to the individual page
                query_temp = url + page_url
                page_data = requests.get(query_temp)
                # Parse and get json script
                soup = BeautifulSoup(page_data.content, "html.parser")
                text = soup.find("script", type="application/ld+json")
                text = json.loads(text.text)
                # Extract graph data from the script
                text = text["@graph"]

                # Initialize lists
                id_list = []
                url_list = []
                format_list = []
                info_list = []
                # Loop thourh dataset info
                for dataset in text:
                    # If the information is about data
                    if dataset["@type"] == 'schema:DataDownload':
                        # Add info to lists
                        id_list.append(dataset["@id"])
                        url_list.append(dataset["schema:url"])
                        info_list.append(dataset["schema:name"])
                        if "schema:encodingFormat" in dataset.keys():
                            format_list.append(dataset[
                                "schema:encodingFormat"])
                        else:
                            format_list.append(None)

                # Extract title
                name = soup.find("head").find("title").string
                # Create temporary DataFrame
                df_temp = pd.DataFrame({"name": [str(name)]*len(id_list),
                                        "page": [str(query_temp)]*len(id_list),
                                        "url": url_list,
                                        "id": id_list,
                                        "format": format_list,
                                        "info": info_list})

                # Add data to main DataFrame
                df = pd.concat([df, df_temp])
        # Reset index
        df = df.reset_index(drop=True)
        # Stop progress bar
        sys.stdout.write("\n")
    else:
        warnings.warn(
            message="Retrieving URL addresses was not successful.",
            category=Warning
            )
    return df


def read_files(file_path, as_df=True, **args):
    """
    Read single CSV or Excel file to pandas.DataFrame.

    Arguments:
        `file_path`: A string specifying the path or URL address to the file.

    Details:
        This function reads files to pd.DataFrame.

    Examples:
        ```
        df_urls = fetch_data_urls("ostolasku")
        df_list = read_files(df_urls.loc[:, "url"])
        ```

    Output:
        pandas.DataFrame or list of pd.DataFrames if file path specifies
        zipped directory with multiple files.

    """
    # INPUT CHECK
    if not (isinstance(file_path, list) or isinstance(file_path, pd.Series)):
        raise Exception(
            "'file_path' must be a list of strings."
            )
    # INPUT CHECK END
    # If the file_path contains only one path, create a list from it
    if isinstance(file_path, str):
        file_path = [file_path]
    # For progress bar, specify the width of it
    progress_bar_width = 50
    # Loop through file paths
    df_list = []
    for i, path_temp in enumerate(file_path):
        # Update the progress bar
        percent = 100*((i+1)/len(file_path))
        sys.stdout.write('\r')
        sys.stdout.write("Completed: [{:{}}] {:>3}%"
                         .format('='*int(percent/(100/progress_bar_width)),
                                 progress_bar_width, int(percent)))
        sys.stdout.flush()
        # Try to load, give a warning if not success
        try:
            df = read_file(path_temp, **args)
            # Convert to list if it is not already
            if not isinstance(df, list):
                df = [df]
            # Add to results
            df_list.extend(df)
        except Exception:
            warnings.warn(
                message=f"Error while loading the following path. "
                f"It is excluded from the output: \n {path_temp}",
                category=UserWarning)
    # Merge list to one DF
    if as_df and len(df_list) > 0:
        res = pd.concat(df_list)
    else:
        res = df_list
    # Stop progress bar
    sys.stdout.write("\n")
    return res


def read_file(file_path, temp_dir=None, **args):
    """
    Read single CSV or Excel file to pandas.DataFrame.

    Arguments:
        `file_path`: A string specifying the path or URL address to the file.

    Details:
        This function reads files to pd.DataFrame.

    Examples:
        ```
        ```

    Output:
        pandas.DataFrame or list of pd.DataFrames if file path specifies
        zipped directory with multiple files.

    """
    # INPUT CHECK
    if not isinstance(file_path, str):
        raise Exception(
            "'file_path' must be a string."
            )
    if not (isinstance(temp_dir, str) or temp_dir is None):
        raise Exception(
            "'temp_dir' must be None or string specifying temporary directory."
            )
    # INPUT CHECK END
    # If the file is not located in the local machine
    if not os.path.exists(file_path):
        # Store the file_path to url variable
        url = file_path
        # If user has not specified temporary directory, get the default
        if temp_dir is None:
            # Get the name of higher level tmp directory
            temp_dir_path = tempfile.gettempdir()
            temp_dir = temp_dir_path + "/osta_tmp_dir"
        # Check if spedicified directory exists. If not, create it
        if not os.path.isdir(temp_dir):
            os.makedirs(temp_dir)
        # Get temporary file path
        file_path = file_path.split("/")
        file_path = file_path[-1]
        file_path = os.path.join(temp_dir, file_path)
        # Try to access the file based on url address if it is not already
        # laoded in cache
        if not os.path.exists(file_path):
            try:
                urllib.request.urlretrieve(url, file_path)
            except Exception:
                raise ValueError(
                    "'file_name' is not a correct path to local file "
                    "and it was not possible to retrieve file based "
                    "on URL address specified by 'file_name'.")

    # Test if the data is in zip format
    file_type = filetype.guess(file_path)
    if file_type is not None and file_type.extension == "zip":
        # Path for extracted files
        extract_path = os.path.join(tempfile.gettempdir(),
                                    "invoice_tempfile_ext.tmp")
        # Remove files from the dir if it already exists
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)
        # Extract files
        with ZipFile(file_path, 'r') as zip:
            zip.extractall(extract_path)
        # Get all the file paths in extracted folder
        file_path = []
        for path, subdirs, files in os.walk(extract_path):
            for name in files:
                file_path.append(os.path.join(path, name))

    # If the file_path contains only one path, create a list from it
    if isinstance(file_path, str):
        file_path = [file_path]

    # Loop over file paths
    dfs = []
    for path_temp in file_path:
        # Open file as DataFrame
        df = __detect_format_and_open_file(path_temp, **args)
        # Store DF tp list
        dfs.append(df)
    # If there is only one DF in list, return only the DF
    if len(dfs) == 1:
        dfs = dfs[0]
    return dfs


def __detect_format_and_open_file(
        file_path, save_dir=None,
        encoding=None, guess_encoding=True,
        delimiter=None, guess_delimiter=True,
        polish_data=True, change_colnames=False,
        **args):
    """
    This function is a helper function to load and detect the format of the
    file that cab be found from the disk and that is not zipped.
    The returned value is pandas.DataFrame.
    """
    # INPUT CHECK
    if not (isinstance(save_dir, str) or save_dir is None):
        raise Exception(
            "'save_dir' must be None or string specifying temporary directory."
            )
    if not (isinstance(encoding, str) or encoding is None):
        raise Exception(
            "'encoding' must be a string or None."
            )
    if not isinstance(guess_encoding, bool):
        raise Exception(
            "'guess_encoding' must be a boolean value."
            )
    if not (isinstance(delimiter, str) or delimiter is None):
        raise Exception(
            "'delimiter' must be a string or None."
            )
    if not isinstance(guess_delimiter, bool):
        raise Exception(
            "'guess_delimiter' must be a boolean value."
            )
    if not isinstance(polish_data, bool):
        raise Exception(
            "'polish_data' must be a boolean value."
            )
    # INPUT CHECK END
    # Check if the file is excel file
    file_type = filetype.guess(file_path)
    if file_type is not None and (file_type.extension == "xls" or
                                  file_type.extension == "xlsx"):
        file_type = "excel"
    elif file_type is None or file_type.extension == "csv":
        # Otherwise try filetype csv unless filetype is guessed to be other
        file_type = "csv"
    else:
        raise Exception(
            "The format of the file is not supported."
            )

    # If encoding is not determined and user wants to guess it for csv file
    if file_type == "csv" and encoding is None and guess_encoding:
        # Open the data
        rawdata = open(file_path, "rb")
        # Check its encoding
        encoding = cchardet.detect(rawdata.read())["encoding"]

    # If delimiter is not specified and user wants to guess it for csv file
    if file_type == "csv" and delimiter is None and guess_delimiter:
        # Open the data
        rawdata = codecs.open(file_path, 'r', encoding=encoding)
        # Get delimiter
        delimiter = csv.Sniffer().sniff(rawdata.read()).delimiter
        # If the delimiter is just space, the file contains empty strings for
        # the missing entries --> empty space is not correct delimiter, try \t
        if delimiter == " ":
            delimiter = "\t"

    # Based on file format, open the file with correct function
    if file_type == "excel":
        df = pd.read_excel(file_path, **args, dtype='unicode')
    else:
        df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter,
                         **args, dtype='unicode')

    # Polish data, i.e., remove empty rows and columns
    if polish_data:
        # Remove spaces from beginning and end of the value
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        # Replace empty strings with None
        df = df.replace(r'^\s*$', None, regex=True)
        # Drop empty rows
        df.dropna(axis=0, how='all', inplace=True)
        # Drop empty columns
        df.dropna(axis=1, how='all', inplace=True)

        # If the first row contained empty values, there are columns named
        # "Unnamed". Replace column names with the values of the first row
        if any([True if x.find('Unnamed') != -1 else False
                for x in df.columns]):
            df.columns = df.iloc[0, :].values
            df = df.iloc[1:, :]

        # If there are columns wih spaces
        bools = [all(df.iloc[:, x[0]].astype(str).str.isspace())
                 for x in enumerate(df.columns)]
        if any(bools):
            df = df.loc[:, [not x for x in bools]]
    # Change colnames if specified
    if change_colnames:
        df = change_names(df, **args)
    # If save_dir is not None, save the file to specified folder
    if save_dir is not None:
        # Check if spedicified directory exists. If not, create it
        if not os.path.isdir(save_dir):
            os.makedirs(save_dir)
        file_path = file_path.split("/")
        file_path = file_path[-1]
        file_path = re.split(r".csv|.xls|.xlsx", file_path)
        file_path = file_path[0]
        file_path = os.path.join(save_dir, file_path)
        # Save file, do not overwrite
        if os.path.exists(file_path + ".csv"):
            i = 0
            while os.path.exists(f"{file_path}{str(i)}.csv"):
                i += 1
            file_path = "file_path_" + str(i) + ".csv"
        file_path = file_path + ".csv"
        df.to_csv(file_path)
    return df
