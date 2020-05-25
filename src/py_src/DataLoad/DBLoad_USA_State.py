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
        Province_State VARCHAR(50) NOT NULL,\
        Country_Region VARCHAR(25) NOT NULL,\
        Last_Update DATE NOT NULL,\
        Lat_ FLOAT NOT NULL,\
        Long_ FLOAT NOT NULL,\
        Confirmed INT NOT NULL,\
        Deaths INT NOT NULL,\
        Recovered INT,\
        Active INT,\
        FIPS INT NOT NULL,\
        Incident_Rate FLOAT,\
        People_Tested INT NOT NULL,\
        People_Hospitalized INT,\
        Mortality_Rate FLOAT,\
        UID INT,\
        ISO3 VARCHAR(3) NOT NULL,\
        Testing_Rate FLOAT,\
        Hospitalization_Rate FLOAT\
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


def main():
    # Create Table
    tableName = "covid_USA_State"
    createTable(tableName)

    # Load Table
    CSVPath = os.getcwd() + '/../../../DataStore/'
    dirName = 'COVID-19-data-state-USA/'
    fileName = 'covid_19_us_states_complete.csv'
    loadCSV = CSVPath + dirName + fileName

    loadCSVsql = "LOAD DATA LOCAL INFILE '" + loadCSV + "'\
        INTO TABLE covid_USA_State\
        FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (\
        Province_State,\
        Country_Region,\
        Last_Update,\
        Lat_,\
        Long_,\
        Confirmed,\
        Deaths,\
        Recovered,\
        Active,\
        FIPS,\
        Incident_Rate,\
        People_Tested,\
        People_Hospitalized,\
        Mortality_Rate,\
        UID,\
        ISO3,\
        Testing_Rate,\
        Hospitalization_Rate);\
        commit;"

    loadTable(loadCSVsql)


if __name__ == "__main__":
    main()
