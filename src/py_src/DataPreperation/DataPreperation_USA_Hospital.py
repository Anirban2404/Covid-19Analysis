#!/usr/bin/env python
# coding: utf-8

# In[1]:

import DirFileOperations
import numpy as np
import pandas as pd


def transformFiles(currDir):
    # Datasets loaded to DataFrame
    df_hospitals = pd.read_csv(currDir + "/Definitive_Healthcare:_USA_Hospital_Beds.csv")
    df_hospitals = df_hospitals.rename(columns={"X": "Long_", "Y": "Lat_"})
    df_hospitals = df_hospitals.drop(columns=['HOSPITAL_TYPE', 'HQ_ADDRESS', 'HQ_ADDRESS1',
                                              'HQ_CITY', 'HQ_STATE', 'HQ_ZIP_CODE',
                                              'Potential_Increase_In_Bed_Capac', 'STATE_FIPS',
                                              'CNTY_FIPS', 'AVG_VENTILATOR_USAGE', 'ADULT_ICU_BEDS',
                                              'PEDI_ICU_BEDS', 'BED_UTILIZATION'])

    # Replace Null values
    df_hospitals = df_hospitals.replace(np.nan, 0, regex=True)
    df_hospitals['FIPS'] = df_hospitals['FIPS'].astype(int)
    df_hospitals['NUM_LICENSED_BEDS'] = df_hospitals['NUM_LICENSED_BEDS'].astype(int)
    df_hospitals['NUM_STAFFED_BEDS'] = df_hospitals['NUM_STAFFED_BEDS'].astype(int)
    df_hospitals['NUM_ICU_BEDS'] = df_hospitals['NUM_ICU_BEDS'].astype(int)
    df_hospitals = df_hospitals[df_hospitals['Lat_'] != 0]
    print("Table Shape: ", df_hospitals.shape)

    return df_hospitals


def main():
    currDir = "../../../DataStore/Hospital-data-US"

    # urls of the files
    # https://coronavirus-resources.esri.com/datasets/1044bb19da8d4dbfb6a96eb1b4ebf629_0/data?geometry=10.019%2C-16.820%2C-34.981%2C72.123
    urls = ['https://opendata.arcgis.com/datasets/1044bb19da8d4dbfb6a96eb1b4ebf629_0.csv']

    fileName = 'hospital_usa_county_wise.csv'

    dpo = DirFileOperations.Dir_File_Operations()
    dpo.create_Dir(currDir)
    dpo.download_Files(urls, currDir)
    usa_hospitals = transformFiles(currDir)
    dpo.saveFiletoCSV(usa_hospitals, currDir, fileName)


if __name__ == "__main__":
    main()
