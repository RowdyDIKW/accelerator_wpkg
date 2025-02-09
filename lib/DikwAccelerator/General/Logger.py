from loguru import logger
from dataclasses import dataclass, field
from pyspark.sql import SparkSession
import datetime
import os

@dataclass()
class Log:
    log_path: str
    spark: SparkSession
    def __post_init__(self):
        self.local_log_directory = "/tmp/TransformParquetFiles_log"
        self.time = f"{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        self.local_log_file = os.path.join(self.local_log_directory, f"notebook_log_{self.time}.log")
    def start(self):
        try:
            os.makedirs(self.local_log_directory, exist_ok=True)
            logger.add(self.local_log_file, rotation="10 MB", retention="7 days", level="DEBUG",
                       format="{time} | {level} | {name} {function} | {message}")

            logger.info(f"Log started")
        except Exception as e:
            logger.error(f"Start log failed: {e}")
            raise

    def close(self):
        try:
            logger.info(f"Transform Parquet files in {self.file_path} process finished")
            with open(self.local_log_file, "r") as file:
                data = file.read()

            # Schrijf naar Lakehouse
            self.spark.createDataFrame([(data,)], ["content"]).coalesce(1).write.text(self.log_path)
        except Exception as e:
            logger.error(f"Close log failed: {e}")
            raise