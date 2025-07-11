from dataclasses import dataclass, field
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import pandas as pd
from DikwAccelerator.General.DqUtils import pandas_clean_old_dates, clean_column_names
from DikwAccelerator.General.GeneralUtils   import pandas_to_spark_dfs, flatten_json
from DikwAccelerator.General.DataClasses    import Schema, Table
from loguru import logger
import datetime
import os
import re
from pyspark.sql.types import TimestampType, DateType

@dataclass
class TransformJsonToDeltaTable:
    table_name : str
    file_path : str
    log_path : str
    schema_name : str
    dest_lh : str
    spark : SparkSession

    def __post_init__(self):
        self.local_log_directory = f"/tmp/TransformJsonFile_{self.table_name}_log"
        self.time = f"{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"

    def execute(self) -> str:
        try:
            os.makedirs(self.local_log_directory, exist_ok=True)
            local_log_file = os.path.join(self.local_log_directory, f"notebook_log_{self.time}.log")
            logger.add(local_log_file, rotation="10 MB", retention="7 days", level="DEBUG",
                       format="{time} | {level} | {name} {function} | {message}")

            logger.info(f"Transform Json file in {self.file_path} process started")
            """         add code below          """

            logger.info(f"create pandas dataframe {self.table_name}")
            # Create df
            pdf = pd.read_json(self.file_path)
            pdf = flatten_json(pdf)
            pdf = pandas_clean_old_dates(pdf)
            logger.info(f"create pandas dataframe {self.table_name} finished")

            # Save as delta tables
            data = {}
            data[self.table_name] = pdf
            data = pandas_to_spark_dfs(data,self.schema_name,self.spark)
            table = Table(table_name=self.table_name, dest_schema=self.schema_name,dest_lakehouse=self.dest_lh,table= data[self.table_name],spark=self.spark)
            table.save()
            """         add code above          """
            logger.info(f"Transform Json files in {self.file_path} process finished")
            with open(local_log_file, "r") as file:
                data = file.read()

            # Schrijf naar Lakehouse
            self.spark.createDataFrame([(data,)], ["content"]).coalesce(1).write.text(self.log_path)


            status = "succes"
            return status
        except Exception as e:
            logger.error(f"Transform Json file {self.file_path} process failed: {e}")
            raise