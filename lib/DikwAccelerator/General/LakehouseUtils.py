import time
from dataclasses import dataclass, field
from typing import Optional, List
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from tqdm.notebook import tqdm
from pyspark.sql import functions as F
from datetime import datetime

class LakehouseUtils:
    def __init__(self, lakehouse_name: str, spark: SparkSession):
        self.lakehouse_name = lakehouse_name
        self.spark = spark

    def save_tables(self,tables: dict, schema = False,match_columns = False):
        try:
            if schema:
                logger.info(f"Creating schema {schema} if it doesnt exist")
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.lakehouse_name}.{schema}")
            if match_columns:
                for key, df in tqdm(tables.items(), desc='Saving tables', unit='Table'):
                    match_columns_present = [col for col in match_columns if col in df.columns]
                    self.save_table((f"{schema}." if schema else "")+key,df,match_columns_present)
            else:
                for key, df in tqdm(tables.items(), desc='Saving tables', unit='Table'):
                    self.write_table((f"{schema}." if schema else "")+key,df)
        except Exception as e:
            logger.error(f"save_tables() failed: {e}")
            raise



    def save_table(self, table_name: str,df: DataFrame, match_columns: list):
        try:
            if self.spark.catalog.tableExists(f"{self.lakehouse_name}.{table_name}"):
                self.merge_table(table_name,df, match_columns)
            else:
                self.write_table(table_name,df)
        except Exception as e:
            logger.error(f"save_table() failed: {e}")
            raise

    def merge_table(self,table_name: str,df: DataFrame, match_columns: list):
        try:
            logger.info(f"Start merging table: {table_name}")
            delta_table = DeltaTable.forName(self.spark, table_name)
            merge_condition = " AND ".join([f"existing.{col} = updates.{col}" for col in match_columns])
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

@dataclass
class Entity:
    entity_name: str
    schema_name: str
    dest_lh: str
    instance: int
    df: DataFrame
    spark: SparkSession
    match_columns: Optional[List[str]] = field(default=None)

    def __post_init__(self):
        self.table_name = f"{self.table_name}"
        self.current_timestamp = datetime.now()
        self.data_columns = self.df.columns
        self.df = self.add_metadata_columns()
        self.metadata_columns = [x for x in self.df.columns if x not in self.data_columns]

    def save_table(self):
        try:
            if self.match_columns:
                self.merge_table()
            else:
                self.write_table()
        except Exception as e:
            logger.error(f"save_table() failed: {e}")
            raise

    def merge_table(self):
        try:
            if self.spark.catalog.tableExists(self.table_name):
                logger.info(f"Start merging table: {self.table_name}")
                existing_table = self.spark.read.table(self.table_name)
                # Ensure existing_table has the same columns as self.df
                df_columns = set(self.df.columns)
                existing_columns = set(existing_table.columns)
                # Add missing columns
                for col in df_columns - existing_columns:
                    existing_table = existing_table.withColumn(col, F.lit(None))
                # Remove extra columns
                existing_table = existing_table.select(*self.df.columns)

                delta_df = self.df.join(
                    existing_table,
                    on=self.data_columns,
                    how='left_anti'
                )
                # aliasen voor leesbaarheid in de merge
                delta_alias = "delta"
                updates_alias = "updates"

                # maak een lijst van condities
                conditions = [f"{delta_alias}.{c} = {updates_alias}.{c}" for c in self.match_columns]

                # combineer alle condities met AND
                merge_condition = " AND ".join(conditions)
                delta_table = DeltaTable.forName(self.spark, self.table_name)

                delta_table.alias(delta_alias).merge(
                delta_df.alias(updates_alias),
                merge_condition
                ).whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

                logger.info(f"Finished merging table: {self.table_name}")
            else:
                logger.info(f"{self.table_name} does not exist yet")
                self.write_table()
        except Exception as e:
            logger.error(f"merge_table() failed: {e}")
            raise

    def write_table(self):
        try:
            logger.info(f"Start writing table: {self.table_name}")
            MAX_RETRIES = 30
            RETRY_DELAY = 3

            for attempt in range(MAX_RETRIES):
                try:
                    self.df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(f"{self.table_name}")
                    break
                except Exception as e:
                    if "ConcurrentAppendException" in str(e) and attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                    else:
                        raise
            logger.info(f"Finished writing table: {self.table_name}")
        except Exception as e:
            logger.error(f"write_table() failed: {e}")
            raise
    

    def add_metadata_columns(self) -> DataFrame:
        df = self.df.withColumn('M_COD_INSTANCE', self.instance)\
                .withColumn("M_UTC_RECORD_INSERTED", self.current_timestamp)\
                .withColumn("M_UTC_START", self.current_timestamp) \
                .withColumn("M_UTC_END", F.lit(None))