#!/usr/bin/env python3
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
        UID INT NOT NULL,\
        iso2 VARCHAR(2) NOT NULL,\
        iso3 VARCHAR(3) NOT NULL,\
        code3 INT NOT NULL,\
        FIPS INT NOT NULL,\
        Admin2 VARCHAR(25),\
        Province_State VARCHAR(50) NOT NULL,\
        Country_Region VARCHAR(25) NOT NULL,\
        Lat_ FLOAT NOT NULL,\
        Long_ FLOAT NOT NULL,\
        Update_Date DATE NOT NULL,\
        Confirmed INT NOT NULL,\
        Deaths INT NOT NULL\
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
    tableName = "covid_USA"
    createTable(tableName)

    # Load Table
    CSVPath = os.getcwd() + '/../../../DataStore/'
    dirName = 'COVID-19-data-US/'
    fileName = 'covid_19_usa_county_wise.csv'
    loadCSV = CSVPath + dirName + fileName

    loadCSVsql = "LOAD DATA LOCAL INFILE '" + loadCSV + "'\
        INTO TABLE covid_USA\
        FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (\
        UID,\
        iso2,\
        iso3,\
        code3,\
        FIPS,\
        Admin2,\
        Province_State,\
        Country_Region,\
        Lat_,\
        Long_,\
        Update_Date,\
        Confirmed,\
        Deaths); "

    loadTable(loadCSVsql)
    countTable(tableName)


if __name__ == "__main__":
    main()
