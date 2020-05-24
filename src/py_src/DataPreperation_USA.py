#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

import wget
import os, datetime
import shutil

import pycountry_convert as pc


# In[2]:


# global csv files
csv_confirmed = "time_series_covid19_confirmed_US.csv"
csv_deaths = "time_series_covid19_deaths_US.csv"


# In[3]:


# urls of the files
urls = ['https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv', 
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv']

# download files
for url in urls:
    filename = wget.download(url)


# In[4]:


currDir = "../../DataStore/COVID-19-data-US"

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


# In[5]:


# download files
for url in urls:
    filename = wget.download(url, currDir)


# In[6]:


# Datasets loaded to DataFrame
df_confirmed = pd.read_csv(currDir + "/time_series_covid19_confirmed_US.csv")
df_deaths = pd.read_csv(currDir + "/time_series_covid19_deaths_US.csv")


# In[7]:


df_confirmed.columns


# In[8]:


ids = df_confirmed.columns[0:11]
us_dates = df_confirmed.columns[11:]

us_conf_df_long = df_confirmed.melt(id_vars=ids, value_vars=us_dates, var_name='Date', value_name='Confirmed')
us_deaths_df_long = df_deaths.melt(id_vars=ids, value_vars=us_dates, var_name='Date', value_name='Deaths')

print(us_conf_df_long.shape)
print(us_deaths_df_long.shape)


# In[9]:


us_conf_df_long.head()


# In[10]:


ft_ids = us_conf_df_long.columns[:-1]
ft_ids


# In[11]:


us_full_table = pd.concat([us_conf_df_long, us_deaths_df_long[['Deaths']]], axis=1)
us_full_table.head()


# In[12]:


us_full_table.loc[us_full_table['Country_Region'] == "US", "Country_Region"] = "USA"


# In[13]:


us_full_table[(us_full_table['Province_State'] == "Maryland" )
                  & (us_full_table['Admin2'] == "Montgomery")] 


# In[14]:


del us_full_table['Combined_Key']
us_full_table.head()


# In[15]:


us_full_table['Date'] = pd.to_datetime(us_full_table.Date)
us_full_table.head()


# In[16]:


us_full_table.to_csv(currDir + '/usa_county_wise.csv', index=False)


# In[17]:


for column in us_full_table.columns:
    print (column)


# In[ ]:




