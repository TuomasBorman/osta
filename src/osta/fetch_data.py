#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import warnings
import sys
import re


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
    # df must be pandas DataFrame
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
