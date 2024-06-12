import urllib
import boto3
import logging
from botocore.client import Config
from urllib.request import urlopen
import json
from typing import Type

logger = logging.getLogger()

logger.setLevel("INFO")
DataSyncerType = Type["DataSyncer"]

class DataSyncer():
    def __init__(self, url: str, s3_bucket: str) -> None:
        self.url = url
        self.census_data = None
        self.s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
        self.s3_bucket = s3_bucket
        self.prefix = "census_data"

        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(s3_bucket)

        opener = urllib.request.build_opener()
        opener.addheaders = [("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")]
        urllib.request.install_opener(opener)


    def extract_census_data(self) -> DataSyncerType:
        logger.info( f"Extracting data from {self.url}")
        with urlopen(f"{self.url}") as response:
            self.census_data = json.loads(response.read())
        logger.info("Data extracted successfully")
        return self


    def load_census_data_to_s3(self) -> None:
        logger.info(f"Uploading census data to S3 bucket {self.s3_bucket}")
        self.bucket.put_object(Key=f"{self.prefix}.json", Body=json.dumps(self.census_data))
        logger.info("Data uploaded successfully")