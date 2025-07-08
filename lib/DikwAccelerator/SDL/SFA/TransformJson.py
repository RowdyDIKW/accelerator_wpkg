from dataclasses import dataclass, field
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import pandas as pd
from DikwAccelerator.General.DqUtils import pandas_clean_old_dates
from DikwAccelerator.General.GeneralUtils   import pandas_to_spark_dfs, flatten_json
from DikwAccelerator.General.DataClasses    import Schema, Table
from loguru import logger
import datetime
import os
import re
from pyspark.sql.types import TimestampType, DateType

@dataclass
class TransformJsonFiles:
    file_path : str
    log_path : str
    schema_name : str
    dest_lh : str
    spark : SparkSession

    def __post_init__(self):
        self.local_log_directory = "/tmp/TransformParquetFiles_log"
        self.time = f"{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"

    def execute(self) -> str:
        try:
            os.makedirs(self.local_log_directory, exist_ok=True)
            local_log_file = os.path.join(self.local_log_directory, f"notebook_log_{self.time}.log")
            logger.add(local_log_file, rotation="10 MB", retention="7 days", level="DEBUG",
                       format="{time} | {level} | {name} {function} | {message}")

            logger.info(f"Transform Parquet files in {self.file_path} process started")
            """         add code below          """
            logger.info(f"get all file paths")
            df = self.spark.read.format("parquet").load(self.file_path).withColumn("file_name", input_file_name())
            file_names = df.select("file_name").distinct().rdd.flatMap(lambda x: x).collect()
            logger.info(f"get all file paths finished")
            data = {}
            for file in file_names:
                # get table name
                logger.info(f"get table name from {file}")
                file_name = os.path.basename(file)
                name_match = re.search(r"[^\.]+\.([^\.]+)\.parquet$", file_name)
                table_name = name_match.group(1)
                logger.info(f"create pandas dataframe {table_name}")
                # Create df
                pdf = pd.read_json(self.file_path+f"/{file_name}")
                pdf = flatten_json(pdf)
                data[table_name] = pandas_clean_old_dates(pdf)
                logger.info(f"create pandas dataframe {table_name} finished")

            # Save as delta tables
            data = pandas_to_spark_dfs(data,self.schema_name,self.spark)
            dataset = Schema(name=self.schema_name, dest=self.dest_lh, dataset=data, spark=self.spark)
            dataset.save()


            """         add code above          """
            logger.info(f"Transform Parquet files in {self.file_path} process finished")
            with open(local_log_file, "r") as file:
                data = file.read()

            # Schrijf naar Lakehouse
            self.spark.createDataFrame([(data,)], ["content"]).coalesce(1).write.text(self.log_path)


            status = "succes"
            return status
        except Exception as e:
            logger.error(f"Transform XLSX file {self.file_path} process failed: {e}")
            raise

@dataclass
class TransformParquetToDeltaTable:
    table_name : str
    file_path : str
    log_path : str
    schema_name : str
    dest_lh : str
    spark : SparkSession

    def __post_init__(self):
        self.local_log_directory = f"/tmp/TransformParquetFile_{self.table_name}_log"
        self.time = f"{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"

    def execute(self) -> str:
        try:
            os.makedirs(self.local_log_directory, exist_ok=True)
            local_log_file = os.path.join(self.local_log_directory, f"notebook_log_{self.time}.log")
            logger.add(local_log_file, rotation="10 MB", retention="7 days", level="DEBUG",
                       format="{time} | {level} | {name} {function} | {message}")

            logger.info(f"Transform Parquet files in {self.file_path} process started")
            """         add code below          """

            logger.info(f"create pandas dataframe {self.table_name}")
            # Create df
            pdf = pd.read_parquet(self.file_path)
            pdf = pandas_clean_old_dates(pdf)
            logger.info(f"create pandas dataframe {self.table_name} finished")

            # Save as delta tables
            data = {}
            data[self.table_name] = pdf
            data = pandas_to_spark_dfs(data,self.schema_name,self.spark)
            table = Table(table_name=self.table_name, dest_schema=self.schema_name,dest_lakehouse=self.dest_lh,table= data[self.table_name],spark=self.spark)
            table.save()
            """         add code above          """
            logger.info(f"Transform Parquet files in {self.file_path} process finished")
            with open(local_log_file, "r") as file:
                data = file.read()

            # Schrijf naar Lakehouse
            self.spark.createDataFrame([(data,)], ["content"]).coalesce(1).write.text(self.log_path)


            status = "succes"
            return status
        except Exception as e:
            logger.error(f"Transform parquet file {self.file_path} process failed: {e}")
            raise