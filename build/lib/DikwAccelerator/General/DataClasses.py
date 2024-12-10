from dataclasses import dataclass, field
from typing import Optional, List
from DikwAccelerator.General.LakehouseUtils import LakehouseUtils
from pyspark.sql import SparkSession
from DikwAccelerator.General.GeneralUtils import print_with_current_datetime
from icecream import ic

@dataclass
class Dataset:
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
        print_with_current_datetime(f'saving dataset: {self.name}')
        if self.key_columns is None:
            self.lh_obj.save_tables(self.dataset,self.name, debug=debug)
        else:
            self.lh_obj.save_tables(self.dataset, self.name, key_columns=self.key_columns, debug=debug)
