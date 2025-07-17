from datetime import datetime

from DikwAccelerator.SDL.SFA.TransformJson import TransformJsonToDeltaTable
from DikwAccelerator.SDL.SFA.TransformParquet import TransformParquetToDeltaTable
from dataclasses import dataclass
from pyspark.sql import SparkSession
from loguru import logger

@dataclass
class TransformToTable:
    table_name: str
    file_path: str
    log_path: str
    schema_name: str
    dest_lh: str
    spark: SparkSession

    def execute(self):
        try:
            logger.info("Transform to table processes started")
            date_dir = datetime.now().strftime("%Y/%m/%d")
            directory = self.file_path
            file_type = directory.rsplit('.', 1)[-1]
            time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

            if file_type == 'json':
                entity_obj = TransformJsonToDeltaTable(
                    table_name=self.table_name,
                    file_path=directory,
                    log_path=self.log_path,
                    schema_name=self.schema_name,
                    dest_lh=self.dest_lh,
                    spark=self.spark
                )
            elif file_type == 'parquet':
                entity_obj = TransformParquetToDeltaTable(
                    table_name=self.table_name,
                    file_path=directory,
                    log_path=self.log_path,
                    schema_name=self.schema_name,
                    dest_lh=self.dest_lh,
                    spark=self.spark
                )
            else:
                print("file type is not supported yet")

            entity_obj.execute()
        except Exception as e:
            logger.error(f"Transform to table {self.file_path} process failed: {e}")
            raise