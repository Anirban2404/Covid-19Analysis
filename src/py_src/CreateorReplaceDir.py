import os
import shutil

# create dir
def create_Dir(currDir):
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
