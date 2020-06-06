#!/usr/bin/env python
# coding: utf-8


import DirFileOperations
import pandas as pd


def transformFiles(currDir):
    # Datasets loaded to DataFrame
    df_population = pd.read_csv(currDir + "/co-est2019-alldata.csv",
                                dtype=str, encoding='latin-1')
    df_population = df_population.loc[:,
                    ['STATE', 'COUNTY', 'STNAME', 'CTYNAME', 'POPESTIMATE2019']]
    df_population['FIPS'] = df_population[['STATE', 'COUNTY']].apply(lambda x: ''.join(x), axis=1)
    df_population['FIPS'] = df_population['FIPS'].astype(int)
    df_population = df_population.loc[:, ['STNAME', 'CTYNAME', 'POPESTIMATE2019', 'FIPS']]
    df_population = df_population.rename(columns={'STNAME': "State", "CTYNAME": "County",
                                                  "POPESTIMATE2019": "Population2019"})
    print(df_population.shape)
    return df_population


def main():
    currDir = "../../../DataStore/Population_County"

    # urls of the files
    urls = [
        'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv']

    fileName = 'population_usa_county_wise.csv'

    dpo = DirFileOperations.Dir_File_Operations()
    dpo.create_Dir(currDir)
    dpo.download_Files(urls, currDir)
    df_population = transformFiles(currDir)
    dpo.saveFiletoCSV(df_population, currDir, fileName)


if __name__ == "__main__":
    main()
