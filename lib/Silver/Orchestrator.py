from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from lib.General.LakehouseUtils import LakehouseUtils
from lib.General.DqUtils import DqUtils

class Orchestrator:
    def __init__(self,spark: SparkSession, bronze_lh_name: str, silver_lh_name):
        self.spark = spark
        self.bronze_lh_name = bronze_lh_name
        self.silver_lh_name = silver_lh_name

    def execute(self):

        bronze_lh_obj = LakehouseUtils(self.bronze_lh_name)
        silver_lh_obj = LakehouseUtils(self.silver_lh_name)
        dq_obj = DqUtils()

        # 1. Get bronze tables
        bronze_tables = bronze_lh_obj.get_all_tables()

        # 2. Prepare data for silver

        silver_tables = dq_obj.deduplicate_tables(bronze_tables)

        # 3. write tables to silver lakehouse

