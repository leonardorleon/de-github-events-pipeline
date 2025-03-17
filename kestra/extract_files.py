import requests
from kestra import Kestra

def extract_gh_archive():
    response = requests.get("https://data.gharchive.org/2015-01-01-15.json.gz", stream=True)

    with open("2015-01-01-15.json.gz", "wb") as file:
        file.write(response.content)

if __name__ == "__main__":
    logger = Kestra.logger()
    
    logger.info("Extracting data from github archive")

    extract_gh_archive()