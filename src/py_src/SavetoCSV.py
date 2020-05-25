# Save to csv file
def saveFiletoCSV(table, currDir, fileName):
    table.to_csv(currDir + '/' + fileName, index=False)
    print("File %s " %fileName, "Saved at %s" % currDir)


