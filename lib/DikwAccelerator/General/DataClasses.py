from dataclasses import dataclass
from DikwAccelerator.General.LakehouseUtils import LakehouseUtils
from pyspark.sql import SparkSession
from DikwAccelerator.General.GeneralUtils import print_with_current_datetime
from icecream import ic

@dataclass
class Dataset:
    """Class for creating a dataset"""
    name: str # name of dataset (this is also used as prefix for all the tables
    dest: str
    key_columns: list
    dataset: dict
    spark: SparkSession

    def __post_init__(self):
        self.lh_obj = LakehouseUtils(self.dest,self.spark)

    def save(self, debug=False) -> None:
        if not debug:
            ic.disable()
        ic()
        print_with_current_datetime(f'saving dataset: {self.name}')
        self.lh_obj.save_tables(self.dataset,self.key_columns,self.name, debug=debug)
