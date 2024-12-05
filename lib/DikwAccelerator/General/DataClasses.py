from dataclasses import dataclass, field
from typing import Optional, List
from DikwAccelerator.General.LakehouseUtils import LakehouseUtils
from pyspark.sql import SparkSession
from DikwAccelerator.General.GeneralUtils import print_with_current_datetime
from icecream import ic
from loguru import logger

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

    def save(self, debug=False) -> None:
        if not debug:
            ic.disable()
        ic()
        logger.info(f'saving dataset schema: {self.name}')
        if self.key_columns is None:
            self.lh_obj.save_tables(self.dataset,schema=self.name, debug=debug)
        else:
            self.lh_obj.save_tables(self.dataset, self.name, key_columns=self.key_columns, debug=debug)
