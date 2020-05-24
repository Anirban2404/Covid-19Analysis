#!/usr/bin/env python
# coding: utf-8
## Retriving Dataset
## https://github.com/CSSEGISandData/COVID-19.git
## ! python3 -m pip install wget
## ! python3 -m pip install pandas
## !python3 -m pip install pycountry_convert 
# In[1]:


import pandas as pd
import numpy as np

import wget
import os, datetime
import shutil

import requests
from bs4 import BeautifulSoup

import glob


# In[2]:


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


# In[3]:


# create dir
def createDir(currDir):
    isdir = os.path.isdir(currDir) 

    if isdir:
        try:
            shutil.rmtree(currDir, ignore_errors=True)
        except OSError:
            print ("Deletition of the directory %s failed" % currDir)

    try:
        os.mkdir(currDir)
    except OSError:
        print ("Creation of the directory %s failed" % currDir)
    else:
        print ("Successfully created the directory %s " % currDir)


# In[4]:


# download files
def downloadFiles(urls, currDir):
    for url in urls:
        wget.download(url, currDir)
    print ("Successfully downloaded files")


# In[5]:


def transformFiles(currDir):
    # get data file names
    filenames = glob.glob(currDir  + "/*.csv")

    dfs = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename))

    # Concatenate all data into one DataFrame
    us_state_table = pd.concat(dfs, ignore_index=True)
    
    # Replace Null values
    us_state_table = us_state_table.replace(np.nan, '', regex=True)
    us_state_table = us_state_table[us_state_table['Province_State'].str.contains('Recovered')!=True]
    print(us_state_table.shape)
    
    return us_state_table


# In[6]:


# Save to csv file
def saveFiletoCSV(usa_state_table, currDir):
    usa_state_table.to_csv(currDir + '/covid_19_us_states_complete.csv', index=False)
    print("File Saved at %s" % currDir)


# In[7]:


#urls for github folder
search_url = "https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports_us"
download_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/"
download_urls = scrape_data(search_url, download_url)

currDir = "../../DataStore/COVID-19-data-state-USA"

createDir(currDir)
downloadFiles(download_urls, currDir)
usa_state_table = transformFiles(currDir)
saveFiletoCSV(usa_state_table, currDir)


# In[ ]:




