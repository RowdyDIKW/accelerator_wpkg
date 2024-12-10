from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from tqdm.notebook import tqdm

class DqUtils:
    def __init__(self):
        self.configs = {}
        self.configs['key_columns'] = ['ID', 'Key']

    def get_configs(self, key: str) -> dict:
        return self.configs.get(key, "Key not found")

    def deduplicate_tables(self, tables: dict) -> dict:
        clean_tables = {}
        for key, df in tqdm(tables.items()):
            key_columns = [col for col in self.configs['key_columns'] if col in df.columns]
            df = df.dropDuplicates()
            if key_columns:
                self.duplicates_check(df, key_columns)
            clean_tables[key] = df
        return clean_tables

    def duplicates_check(self, df: DataFrame, key_columns: list):
        duplicates_df = df.groupBy(key_columns).agg(F.count("*").alias("count")).filter(F.col("count") > 1)
        if duplicates_df.count() > 0:
            raise ValueError("Duplicates found for one or more of the key columns")