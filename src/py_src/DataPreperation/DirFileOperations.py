import os
import shutil
import wget

class Dir_File_Operations:
    # create dir
    def create_Dir(self, currDir):
        isdir = os.path.isdir(currDir)

        if isdir:
            try:
                shutil.rmtree(currDir, ignore_errors=True)
            except OSError:
                print("Deletion of the directory %s failed" % currDir)

        try:
            os.mkdir(currDir)
        except OSError:
            print("Creation of the directory %s failed" % currDir)
        else:
            print("Successfully created the directory %s " % currDir)

    def download_Files(self, urls, currDir):
        for url in urls:
            wget.download(url, currDir)
        print("Successfully downloaded files")

    def saveFiletoCSV(self, table, currDir, fileName):
        table.to_csv(currDir + '/' + fileName, index=False)
        print("File %s " % fileName, "Saved at %s" % currDir)