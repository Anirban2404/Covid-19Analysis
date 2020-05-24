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


# In[2]:


import requests
from bs4 import BeautifulSoup


# In[3]:


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


# In[4]:


#urls for github folder
search_url = "https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports_us"
download_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/"
download_urls = scrape_data(search_url, download_url)


# In[5]:


currDir = "../../DataStore/COVID-19-data-state-USA"

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


# In[6]:


# download files
for url in download_urls:
    filename = wget.download(url, currDir)


# In[7]:


import glob

# get data file names
filenames = glob.glob(currDir  + "/*.csv")

dfs = []
for filename in filenames:
    dfs.append(pd.read_csv(filename))

# Concatenate all data into one DataFrame
big_frame = pd.concat(dfs, ignore_index=True)


# In[8]:


big_frame = big_frame.replace(np.nan, '', regex=True)
big_frame = big_frame[big_frame['Province_State'].str.contains('Recovered')!=True]


# In[9]:


big_frame.to_csv(currDir + '/covid_19_us_states_complete.csv', index=False)


# In[10]:


for column in big_frame.columns:
    print (column)


# In[ ]:





# In[ ]:




