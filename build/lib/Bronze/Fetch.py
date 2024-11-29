from pyspark.sql import SparkSession
import requests
import xml.etree.ElementTree as ET
import json
from DikwAccelerator.General.GeneralUtils import print_with_current_datetime,indent

class FetchCbsData:
    def __init__(self,spark: SparkSession, data_set_code: str):
        self.data_set_code = data_set_code
        self.base_url = f"https://opendata.cbs.nl/ODataFeed/OData/{data_set_code}NED"
        self.spark = spark
        self.lh_path = "/lakehouse/default/Files/"

    def fetch_and_store_data(self):
        print_with_current_datetime("START FETCHING AND STORING ALL DATASETS")
        print_with_current_datetime(indent + "start fetching dataset urls")
        dataset_urls = self.fetch_dataset_urls()
        print_with_current_datetime(indent + "finished fetching dataset urls")
        for dataset_url in dataset_urls:
            dataset = self.fetch_dataset(dataset_url)
            file_name = dataset_url.split('/')[-1] + ".json"
            self.store_dataset(dataset,file_name)
        print_with_current_datetime("FINISHED FETCHING AND STORING ALL DATASETS")


    def fetch_dataset_urls(self) -> list:
        response = requests.get(self.base_url)
        root = ET.fromstring(response.content)
        namespace = {'atom': root.tag.split('}')[0].strip('{')}

        dataset_urls = []
        for collection in root.findall('.//atom:collection', namespace):
            dataset_urls.append(collection.attrib['href'])
        return dataset_urls

    def fetch_dataset(self, dataset_url:str) -> json:
        print_with_current_datetime(indent + f"fetching dataset from: {dataset_url}")
        response = requests.get(f"{dataset_url}?$format=json")
        if response.status_code == 200:
            json_data = response.json()

            data_value = json_data.get('value', [])
        else:
            raise ValueError(f"Failed to fetch dataset from: {dataset_url}")
        return data_value

    def store_dataset(self, dataset: json, file_name: str):
        print_with_current_datetime(indent + "writing " + file_name)
        with open(f"{self.lh_path}{file_name}", 'w') as json_file:
            json.dump(dataset, json_file)
        print_with_current_datetime(indent + "stored " + file_name)