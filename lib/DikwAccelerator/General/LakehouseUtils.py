import time

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from tqdm.notebook import tqdm

class LakehouseUtils:
    def __init__(self, lakehouse_name: str, spark: SparkSession):
        self.lakehouse_name = lakehouse_name
        self.spark = spark

    def save_tables(self,tables: dict, schema = False,key_columns = False):
        try:
            if schema:
                logger.info(f"Creating schema {schema} if it doesnt exist")
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.lakehouse_name}.{schema}")
            if key_columns:
                for key, df in tqdm(tables.items(), desc='Saving tables', unit='Table'):
                    key_columns_present = [col for col in key_columns if col in df.columns]
                    self.save_table((f"{schema}." if schema else "")+key,df,key_columns_present)
            else:
                for key, df in tqdm(tables.items(), desc='Saving tables', unit='Table'):
                    self.write_table((f"{schema}." if schema else "")+key,df)
        except Exception as e:
            logger.error(f"save_tables() failed: {e}")
            raise



    def save_table(self, table_name: str,df: DataFrame, key_columns: list):
        try:
            if self.spark.catalog.tableExists(f"{self.lakehouse_name}.{table_name}"):
                self.merge_table(table_name,df, key_columns)
            else:
                self.write_table(table_name,df)
        except Exception as e:
            logger.error(f"save_table() failed: {e}")
            raise

    def merge_table(self,table_name: str,df: DataFrame, key_columns: list):
        try:
            logger.info(f"Start merging table: {table_name}")
            delta_table = DeltaTable.forName(self.spark, table_name)
            merge_condition = " AND ".join([f"existing.{col} = updates.{col}" for col in key_columns])
            delta_table.alias("existing").merge(source=df.alias("updates"),condition=merge_condition)\
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            logger.info(f"Finished merging table: {table_name}")
        except Exception as e:
            logger.error(f"merge_table() failed: {e}")
            raise

    def write_table(self, table_name: str,df: DataFrame):
        try:
            logger.info(f"Start writing table: {table_name}")
            MAX_RETRIES = 30
            RETRY_DELAY = 3

            for attempt in range(MAX_RETRIES):
                try:
                    df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(f"{self.lakehouse_name}.{table_name}")
                    break
                except Exception as e:
                    if "ConcurrentAppendException" in str(e) and attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                    else:
                        raise
            logger.info(f"Finished writing table: {table_name}")
        except Exception as e:
            logger.error(f"write_table() failed: {e}")
            raise
