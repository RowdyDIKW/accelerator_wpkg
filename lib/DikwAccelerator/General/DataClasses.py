from dataclasses import dataclass, field
from typing import Optional, List
from DikwAccelerator.General.LakehouseUtils import LakehouseUtils
from DikwAccelerator.General.GeneralUtils import auto_cast_dataframe
from pyspark.sql import SparkSession
from DikwAccelerator.General.GeneralUtils import print_with_current_datetime
from icecream import ic
from loguru import logger
from tqdm.notebook import tqdm

@dataclass
class Schema:
    """Class for creating a dataset"""
    name: str # name of dataset (this is also used as prefix for all the tables
    dest: str
    dataset: dict
    spark: SparkSession
    key_columns: Optional[List[str]] = field(default=None)

    def __post_init__(self):
        self.lh_obj = LakehouseUtils(self.dest,self.spark)

    def save(self) -> None:
        try:
            logger.info(f'saving dataset schema: {self.name}')
            if self.key_columns is None:
                self.lh_obj.save_tables(self.dataset,schema=self.name)
            else:
                self.lh_obj.save_tables(self.dataset, self.name, key_columns=self.key_columns)
        except Exception as e:
            logger.error(f"saving dataset schema: {self.name} failed: {e}")
            raise
    def assign_datatypes(self):
        try:
            logger.info(f'Start assigning datatypes to schema: {self.name}')
            for table_name, df in tqdm(self.dataset.items()):
                logger.info(f'assigning datatype to {table_name}')
                self.dataset[table_name] = auto_cast_dataframe(df)
            logger.info(f'Finished assigning datatypes to schema: {self.name}')
        except Exception as e:
            logger.error(f"assigning datatypes to schema: {self.name} failed: {e}")
            raise