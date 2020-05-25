#!/usr/bin/env python
# coding: utf-8


import DirFileOperations
import pandas as pd


def transformFiles(currDir):
    # Datasets loaded to DataFrame
    df_confirmed = pd.read_csv(currDir + "/time_series_covid19_confirmed_US.csv")
    df_deaths = pd.read_csv(currDir + "/time_series_covid19_deaths_US.csv")
    # print(df_confirmed.columns)

    ids = df_confirmed.columns[0:11]
    us_dates = df_confirmed.columns[11:]

    us_conf_df_long = df_confirmed.melt(id_vars=ids, value_vars=us_dates,
                                        var_name='Date', value_name='Confirmed')
    us_deaths_df_long = df_deaths.melt(id_vars=ids, value_vars=us_dates,
                                       var_name='Date', value_name='Deaths')

    # print(us_conf_df_long.shape)
    # print(us_deaths_df_long.shape)

    # ft_ids = us_conf_df_long.columns[:-1]
    us_full_table = pd.concat([us_conf_df_long, us_deaths_df_long[['Deaths']]], axis=1)
    del us_full_table['Combined_Key']
    us_full_table['Date'] = pd.to_datetime(us_full_table.Date)
    us_full_table.loc[us_full_table['Country_Region'] == "US", "Country_Region"] = "USA"

    us_full_table['Confirmed'] = us_full_table['Confirmed'].astype(int)
    us_full_table['Deaths'] = us_full_table['Deaths'].astype(int)
    print(us_full_table.shape)

    return us_full_table


# Save to csv file
def saveFiletoCSV(usa_full_table, currDir):
    usa_full_table.to_csv(currDir + '/covid_19_usa_county_wise.csv', index=False)
    print("File Saved at %s" % currDir)


def main():
    currDir = "../../DataStore/COVID-19-data-US"

    # urls of the files
    urls = [
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv',
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv']

    fileName = 'covid_19_usa_county_wise.csv'

    dpo = DirFileOperations.Dir_File_Operations()
    dpo.create_Dir(currDir)
    dpo.download_Files(urls, currDir)
    usa_full_table = transformFiles(currDir)
    dpo.saveFiletoCSV(usa_full_table, currDir, fileName)


if __name__ == "__main__":
    main()

# usa_full_table[(usa_full_table['Province_State'] == "Maryland" )
#                   & (usa_full_table['Admin2'] == "Montgomery")]
