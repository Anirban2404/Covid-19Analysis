#!/usr/bin/env python
# coding: utf-8
# !python3 -m pip install mysql-connector-python-rf
# !python3 -m pip install mysql-connector-python


import os

import DBConnect


def createTable(tableName):
    dropTable = "DROP TABLE " + tableName + ";"
    createTable = "CREATE TABLE " + tableName + "(\
        Long_ FLOAT NOT NULL,\
        Lat_ FLOAT NOT NULL,\
        FID INT NOT NULL,\
        HOSPITAL_NAME VARCHAR(100) NOT NULL,\
        COUNTY_NAME VARCHAR(25) NOT NULL,\
        STATE_NAME VARCHAR(25) NOT NULL,\
        FIPS INT NOT NULL,\
        NUM_LICENSED_BEDS INT NOT NULL,\
        NUM_STAFFED_BEDS INT NOT NULL,\
        NUM_ICU_BEDS INT NOT NULL\
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
    tableName = "hospital_USA"
    createTable(tableName)

    # Load Table
    CSVPath = os.getcwd() + '/../../../DataStore/'
    dirName = 'Hospital-data-US/'
    fileName = 'hospital_usa_county_wise.csv'
    loadCSV = CSVPath + dirName + fileName

    loadCSVsql = "LOAD DATA LOCAL INFILE '" + loadCSV + "'\
        INTO TABLE hospital_USA\
        FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (\
        Long_,\
        Lat_,\
        FID,\
        HOSPITAL_NAME,\
        COUNTY_NAME,\
        STATE_NAME,\
        FIPS,\
        NUM_LICENSED_BEDS,\
        NUM_STAFFED_BEDS,\
        NUM_ICU_BEDS); "

    loadTable(loadCSVsql)
    countTable(tableName)


if __name__ == "__main__":
    main()
