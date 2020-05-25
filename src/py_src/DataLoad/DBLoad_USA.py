#!/usr/bin/env python
# coding: utf-8
# !python3 -m pip install mysql-connector-python-rf
# !python3 -m pip install mysql-connector-python
# In[1]:


import mysql.connector
import os


# In[2]:


'''
allow local infile loading must be TRUE to load csv files.

>>> mysql> SHOW GLOBAL VARIABLES LIKE '%infile%';
    +---------------+-------+
    | Variable_name | Value |
    +---------------+-------+
    | local_infile  | OFF   |
    +---------------+-------+
    1 row in set (0.00 sec)

>>> mysql> SET GLOBAL local_infile=1;
    Query OK, 0 rows affected (0.00 sec)

>>> mysql> SHOW GLOBAL VARIABLES LIKE '%infile%';
    +---------------+-------+
    | Variable_name | Value |
    +---------------+-------+
    | local_infile  | ON    |
    +---------------+-------+
    1 row in set (0.00 sec)
'''

class DBConnect:
    def __init__(self, host):
        self.host = host

    def _mySqlConnect(self):
        _mySqlDB = mysql.connector.connect(
            host=self.host,  # "localhost",
            user="covidAnalyst",
            passwd="P@ssw0rd",
            auth_plugin='mysql_native_password',
            database="covid_data",
            allow_local_infile=True
        )
        return _mySqlDB


# In[3]:


def createTable(tableName):
    dropTable = "DROP TABLE " + tableName + ";"
    createTable = "CREATE TABLE " + tableName + "(        UID INT NOT NULL,        iso2 VARCHAR(2) NOT NULL,        iso3 VARCHAR(3) NOT NULL,        code3 INT NOT NULL,        FIPS FLOAT NOT NULL,        Admin2 VARCHAR(25),        Province_State VARCHAR(50) NOT NULL,        Country_Region VARCHAR(25) NOT NULL,        Lat_ FLOAT NOT NULL,        Long_ FLOAT NOT NULL,        Update_Date DATE NOT NULL,        Confirmed INT NOT NULL,        Deaths INT NOT NULL    );"
    
    # Connect to covid_data database
    covid_db = DBConnect(host="localhost")
     
    mySqlConnect = covid_db._mySqlConnect()
    cursor = mySqlConnect.cursor()
    
    try:
        cursor.execute(dropTable)
    except:
        print ("Drop Table failed")
        cursor.execute(createTable)
        print ("Successfully created the table")
    else:
        cursor.execute(createTable)
        print ("Successfully created the table")
    cursor.close()


# In[4]:


def loadTable(loadTable):
    # Connect to covid_data database
    covid_db = DBConnect(host="localhost")
     
    mySqlConnect = covid_db._mySqlConnect()
    cursor = mySqlConnect.cursor()
    
    try:
        cursor.execute(loadTable)
        print ("Successfully loaded the table")
    except Exception:
        print ("Load Table failed", Exception)
    cursor.close()


# In[5]:


# Create Table
tableName = "covid_USA"
createTable(tableName)

# Load Table
CSVPath = os.getcwd() + '/../../DataStore/'
dirName = 'COVID-19-data-US/'
fileName = 'covid_19_usa_county_wise.csv'
loadCSV = CSVPath + dirName + fileName 

loadCSVsql = "LOAD DATA LOCAL INFILE '" + loadCSV + "'    INTO TABLE covid_USA    FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (    UID,    iso2,    iso3,    code3,    FIPS,    Admin2,    Province_State,    Country_Region,    Lat_,    Long_,    Update_Date,    Confirmed,    Deaths);     commit;"

loadTable(loadCSVsql)


# In[ ]:




