import wget

# download files
def download_Files(urls, currDir):
    for url in urls:
        wget.download(url, currDir)
    print("Successfully downloaded files")


