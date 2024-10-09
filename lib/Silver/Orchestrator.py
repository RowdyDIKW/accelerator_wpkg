from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from General.LakehouseUtils import LakehouseUtils
from General.DqUtils import DqUtils

class Orchestrator:
    def __init__(self,spark: SparkSession, bronze_lh_name: str, silver_lh_name):
        self.spark = spark
        self.bronze_lh_name = bronze_lh_name
        self.silver_lh_name = silver_lh_name

    def execute(self):

        bronze_lh_obj = LakehouseUtils(self.bronze_lh_name, self.spark)
        silver_lh_obj = LakehouseUtils(self.silver_lh_name, self.spark)
        dq_obj = DqUtils()

        # 1. Get bronze tables
        print("get all tables from bronze layer")
        bronze_tables = bronze_lh_obj.get_all_tables()

        # 2. Prepare data for silver
        print("prepare all tables for silver layer")
        silver_tables = dq_obj.deduplicate_tables(bronze_tables)

        # 3. write tables to silver lakehouse
        print("store tables in silver layer lakehouse")
        silver_lh_obj.save_tables(silver_tables,dq_obj.configs['key_columns'])