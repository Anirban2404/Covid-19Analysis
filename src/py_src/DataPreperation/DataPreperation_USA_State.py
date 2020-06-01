#!/usr/bin/env python

'''
# coding: utf-8
Retriving Dataset
https://github.com/CSSEGISandData/COVID-19.git
! python3 -m pip install wget
! python3 -m pip install pandas
!python3 -m pip install pycountry_convert
'''

import glob

import DirFileOperations
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


def scrape_data(search_url, download_url):
    response = requests.get(search_url, timeout=10)
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find_all('table')
    rows = table[0].select('tbody > tr > td > span ')

    header = [th.text.rstrip() for th in rows[0].find_all('th')]
    download_urls = []
    for row in rows:
        for th in row.find_all('a', href=True):
            if 'csv' in th['title'] and len(th['title']) < 15:
                download_urls.append(download_url + th['title'].rstrip())
    return download_urls


def transformFiles(currDir):
    # get data file names
    filenames = glob.glob(currDir + "/*.csv")

    dfs = []
    for filename in filenames:
        tmp = pd.read_csv(filename)
        # Changed the Last_Update date according to filename
        tmp['Last_Update'] = filename[43:53]
        tmp['Last_Update'] = pd.to_datetime(tmp.Last_Update)
        dfs.append(tmp)

    # Concatenate all data into one DataFrame
    us_state_table = pd.concat(dfs, ignore_index=True)

    # Replace Null values
    us_state_table = us_state_table.replace(np.nan, '', regex=True)
    us_state_table = us_state_table[
        us_state_table['Province_State'].str.contains('Recovered') != True]
    print(us_state_table.shape)
    us_state_table = us_state_table.sort_values(by='Last_Update')

    return us_state_table


def main():
    # urls for github folder
    search_url = "https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports_us"
    download_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/"

    download_urls = scrape_data(search_url, download_url)

    currDir = "../../../DataStore/COVID-19-data-state-USA"
    fileName = "covid_19_us_states_complete.csv"

    dpo = DirFileOperations.Dir_File_Operations()
    dpo.create_Dir(currDir)
    dpo.download_Files(download_urls, currDir)
    usa_state_table = transformFiles(currDir)
    dpo.saveFiletoCSV(usa_state_table, currDir, fileName)


if __name__ == "__main__":
    main()
