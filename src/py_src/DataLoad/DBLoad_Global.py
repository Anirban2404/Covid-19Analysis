#!/usr/bin/env python
# coding: utf-8

'''
!python3 -m pip install mysql-connector-python-rf
!python3 -m pip install mysql-connector-python
'''

import os

import DBConnect


def createTable(tableName):
    dropTable = "DROP TABLE " + tableName + ";"
    createTable = "CREATE TABLE " + tableName + "(\
        Province_State VARCHAR(50),\
        Country_Region VARCHAR(25) NOT NULL,\
        Continent VARCHAR(25) NOT NULL,\
        Lat_ FLOAT NOT NULL,\
        Long_ FLOAT NOT NULL,\
        Update_Date DATE NOT NULL,\
        Confirmed INT NOT NULL,\
        Deaths INT NOT NULL,\
        Recovered INT\
    );"

    # Connect to covid_data database
    covid_db = DBConnect._DBConnect(host="localhost")

    mySqlConnect = covid_db._mySqlConnect()
    cursor = mySqlConnect.cursor()
    try:
        cursor.execute(dropTable)
    except:
        print("Drop Table %s failed" % tableName)
        cursor.execute(createTable)
        print("Successfully created the table %s" % tableName)
    else:
        cursor.execute(createTable)
        print("Successfully created the table %s" % tableName)
    cursor.close()


def loadTable(loadCSVsql):
    # Connect to covid_data database
    covid_db = DBConnect._DBConnect(host="localhost")

    mySqlConnect = covid_db._mySqlConnect()
    cursor = mySqlConnect.cursor()

    try:
        cursor.execute(loadCSVsql)
        print("Successfully loaded the table")
    except Exception:
        print("Load Table failed", Exception)
    cursor.close()


def countTable(tableName):
    # Connect to covid_data database
    covid_db = DBConnect._DBConnect(host="localhost")

    mySqlConnect = covid_db._mySqlConnect()
    cursor = mySqlConnect.cursor()
    countQuery = "select * from " + tableName + ";"
    cursor.execute(countQuery)
    rows = cursor.fetchall()
    print('Total Row(s):', cursor.rowcount)
    cursor.close()


def main():
    # Create Table
    tableName = 'covid_global'
    createTable(tableName)

    # Load Table
    CSVPath = os.getcwd() + '/../../../DataStore/'
    dirName = 'COVID-19-global/'
    fileName = 'covid_19_global_complete.csv'
    loadCSV = CSVPath + dirName + fileName

    loadCSVsql = "LOAD DATA LOCAL INFILE '" + loadCSV + "'\
        INTO TABLE covid_global\
        FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (\
        Province_State,\
        Country_Region,\
        Continent,\
        Lat_,\
        Long_,\
        Update_Date,\
        Confirmed,\
        Deaths,\
        Recovered); "

    loadTable(loadCSVsql)
    countTable(tableName)


if __name__ == "__main__":
    main()
