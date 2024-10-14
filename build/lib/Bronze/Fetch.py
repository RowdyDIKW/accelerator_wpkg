from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import requests
import xml.etree.ElementTree as ET
import json
from General.GeneralUtils import print_with_current_datetime,indent

class FetchCbsData:
    def __init__(self,spark: SparkSession, data_set_code: str, lh_name: str):
        self.data_set_code = data_set_code
        self.base_url = f"https://opendata.cbs.nl/ODataFeed/OData/{data_set_code}NED"
        self.spark = spark
        self.lh_name = lh_name

    def fetch_and_convert_to_df(self, dataset_name: str):
        url = f"{self.base_url}/{dataset_name}"
        response = requests.get(url)
        data = response.json()
        rdd = self.spark.sparkContext.parallelize(data['value'])
        return self.spark.read.json(rdd)

    def fetch_and_store_data(self):
        print_with_current_datetime("START FETCHING AND STORING ALL DATASETS")
        print_with_current_datetime(indent + "start fetching dataset urls")
        dataset_urls = self.fetch_dataset_urls()
        print_with_current_datetime(indent + "finished fetching dataset urls")


    def fetch_dataset_urls(self) -> list:
        response = requests.get(self.base_url)
        root = ET.fromstring(response.content)
        namespace = {'atom': root.tag.split('}')[0].strip('{')}

        dataset_urls = []
        for collection in root.findall('.//atom:collection', namespace):
            dataset_urls.append(collection.attrib['href'])
        return dataset_urls

    def fetch_dataset(self, dataset_url:str):
        print_with_current_datetime(indent + f"fetching dataset from: {dataset_url}")
        response = requests.get(f"{dataset_url}?$format=json")
        if response.status_code == 200:
            json_data = response.json()

            data_value = json_data.get('value', [])
        else:

