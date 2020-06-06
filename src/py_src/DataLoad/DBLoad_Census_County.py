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
        State VARCHAR(50),\
        County VARCHAR(50) NOT NULL,\
        Population2019 INT NOT NULL,\
        FIPS INT NOT NULL\
    );"

    # Connect to covid_data database
    covid_db = DBConnect._DBConnect(host="localhost")

    mySqlConnect = covid_db._mySqlConnect()
    cursor = mySqlConnect.cursor()
    try:
        cursor.execute(dropTable)
    except:
        print("Drop Table failed")
        cursor.execute(createTable)
        print("Successfully created the table")
    else:
        cursor.execute(createTable)
        print("Successfully created the table")
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
    tableName = 'population_usa_county'
    createTable(tableName)

    # Load Table
    CSVPath = os.getcwd() + '/../../../DataStore/'
    dirName = 'Population_County/'
    fileName = 'population_usa_county_wise.csv'
    loadCSV = CSVPath + dirName + fileName

    loadCSVsql = "LOAD DATA LOCAL INFILE '" + loadCSV + "'\
        INTO TABLE population_usa_county\
        FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (\
        State,\
        County,\
        Population2019,\
        FIPS);"

    loadTable(loadCSVsql)
    countTable(tableName)


if __name__ == "__main__":
    main()
