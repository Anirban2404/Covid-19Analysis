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
import pycountry_convert as pc


# In[2]:


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


# In[3]:


# download files
def downloadFiles(urls, currDir):
    for url in urls:
        wget.download(url, currDir)
    print ("Successfully downloaded files")


# In[4]:


# Defininng Function for getting continent code for country.
def country_to_continent_code(country):
    try:
        return pc.country_alpha2_to_continent_code(pc.country_name_to_country_alpha2(country))
    except :
        return 'na'


# In[5]:


# function to change value
def change_val(full_table, date, ref_col, val_col, dtnry):
    for key, val in dtnry.items():
        full_table.loc[(full_table['Date']==date) & (full_table[ref_col]==key), val_col] = val


# In[6]:


def transformFiles(currDir):
    # Datasets loaded to DataFrame
    df_confirmed = pd.read_csv(currDir + "/time_series_covid19_confirmed_global.csv")
    df_deaths = pd.read_csv(currDir + "/time_series_covid19_deaths_global.csv")
    df_recovered = pd.read_csv(currDir + "/time_series_covid19_recovered_global.csv")
    # print(df_confirmed.columns)

    dates = df_confirmed.columns[4:]

    conf_df_long = df_confirmed.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                value_vars=dates, var_name='Date', value_name='Confirmed')

    deaths_df_long = df_deaths.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                value_vars=dates, var_name='Date', value_name='Deaths')

    recv_df_long = df_recovered.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                value_vars=dates, var_name='Date', value_name='Recovered')

    recv_df_long = recv_df_long[recv_df_long['Country/Region']!='Canada']
    """
    print("Confirmed Shape: ", conf_df_long.shape)
    print("Deaths Shape: ", deaths_df_long.shape)
    print("Recovered Shape: ", recv_df_long.shape)
    """
    full_table = pd.merge(left=conf_df_long, right=deaths_df_long, how='left',
                      on=['Province/State', 'Country/Region', 'Date', 'Lat', 'Long'])
    full_table = pd.merge(left=full_table, right=recv_df_long, how='left',
                          on=['Province/State', 'Country/Region', 'Date', 'Lat', 'Long'])

    # Changing the conuntry names as required by pycountry_convert Lib
    full_table.loc[full_table['Country/Region'] == "US", "Country/Region"] = "USA"
    full_table.loc[full_table['Country/Region'] == 'Korea, South', "Country/Region"] = 'South Korea'
    full_table.loc[full_table['Country/Region'] == 'Taiwan*', "Country/Region"] = 'Taiwan'
    full_table.loc[full_table['Country/Region'] == 'Congo (Kinshasa)', "Country/Region"] = 'Democratic Republic of the Congo'
    full_table.loc[full_table['Country/Region'] == 'Congo (Brazzaville)', "Country/Region"] = 'Republic of the Congo'
    full_table.loc[full_table['Country/Region'] == 'Bahamas, The', "Country/Region"] = 'Bahamas'
    full_table.loc[full_table['Country/Region'] == 'Gambia, The', "Country/Region"] = 'Gambia'
    full_table.loc[full_table['Country/Region'] == "Cote d'Ivoire", "Country/Region"] = "Côte d'Ivoire"
    full_table.loc[full_table['Country/Region'] == "Reunion", "Country/Region"] = "Réunion"
    print("Full table Shape: ", full_table.shape)

    # Replace Null
    full_table['Recovered'] = full_table['Recovered'].fillna(0)
    full_table['Recovered'] = full_table['Recovered'].astype('int')

    # getting all countries
    countries = np.asarray(full_table["Country/Region"])

    # Continent_code to Continent_names
    continents = {
        'NA': 'North America',
        'SA': 'South America', 
        'AS': 'Asia',
        'OC': 'Australia',
        'AF': 'Africa',
        'EU' : 'Europe',
        'na' : 'Others'
    }

    # Collecting Continent Information
    full_table.insert(2,"Continent", [continents[country_to_continent_code(country)] for country in countries[:]] ) 

    # removing canada's recovered values
    full_table = full_table[full_table['Province/State'].str.contains('Recovered')!=True]

    # replace Bonaire, Sint Eustatius and Saba
    full_table.loc[full_table['Province/State'] == "Bonaire, Sint Eustatius and Saba", "Province/State"] = "Caribbean Netherlands"
    # print("Full table Shape: ", full_table.shape)

    # Fixing off data
    # new values
    feb_12_conf = {'Hubei' : 34874}

    # changing values
    change_val(full_table, '2/12/20', 'Province/State', 'Confirmed', feb_12_conf)
    
    # checking values
    # print(full_table[(full_table['Date']=='2/12/20') & (full_table['Province/State']=='Hubei')])
    
    # Replace Null values
    full_table = full_table.replace(np.nan, '', regex=True)
    full_table['Date'] = pd.to_datetime(full_table.Date)
    # print(full_table.head())
    
    return full_table


# In[7]:


# Save to csv file
def saveFiletoCSV(global_table, currDir):
    global_table.to_csv(currDir + '/covid_19_global_complete.csv', index=False)
    print("File Saved at %s" % currDir)


# In[8]:


# global csv files
# csv_confirmed = "time_series_covid19_confirmed_global.csv"
# csv_deaths = "time_series_covid19_deaths_global.csv"
# csv_recovered = "time_series_covid19_recovered_global.csv"
# urls of the files
urls = ['https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv', 
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv']

currDir = "../../DataStore/COVID-19-global"

createDir(currDir)
downloadFiles(urls, currDir)
global_table = transformFiles(currDir)
saveFiletoCSV(global_table, currDir)


# In[ ]:




