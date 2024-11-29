from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from tqdm.notebook import tqdm
from icecream import ic

class LakehouseUtils:
    def __init__(self, lakehouse_name: str, spark: SparkSession):
        self.lakehouse_name = lakehouse_name
        self.spark = spark
        self.tables = spark.catalog.listTables(self.lakehouse_name)
        self.lh_metadata = {
            'table_names': [table.name for table in self.tables]
        }
        self.lh_metadata['schemas'] = {}
        for name in self.lh_metadata['table_names']:
            columns = spark.catalog.listColumns(f'{self.lakehouse_name}.{name}')
            schema_dict = {col.name: col.dataType for col in columns}
            self.lh_metadata['schemas'][name] = schema_dict

    def get_meta_data(self, key: str) -> dict:
        return self.lh_metadata.get(key, "Key not found")

    def get_table_names(self) -> list:
        return self.get_meta_data('table_names')

    def get_table_schema(self, table_name: str) -> dict:
        return self.get_meta_data('schemas')[table_name]

    def get_table(self, table_name: str) -> DataFrame:
        df = self.spark.table(f"{self.lakehouse_name}.{table_name}")
        return df

    def get_all_tables(self, debug=False) -> dict:
        if not debug:
            ic.disable()
        ic()
        tables = {}
        for table in self.get_table_names():
            ic(table)
            tables[table] = self.get_table(table)
        return tables

    def save_tables(self,tables: dict, prefix: str,key_columns = False, debug=False):
        if not debug:
            ic.disable()
        ic()
        if key_columns:
            for key, df in tqdm(tables.items(), desc='Saving tables', unit='Table'):
                ic(key)
                key_columns_present = [col for col in key_columns if col in df.columns]
                self.save_table(prefix+"_"+key,df,key_columns_present)
        else:
            for key, df in tqdm(tables.items(), desc='Saving tables', unit='Table'):
                ic(key)
                self.write_table(prefix+"_"+key,df)



    def save_table(self, table_name: str,df: DataFrame, key_columns: list):
        if self.spark.catalog.tableExists(f"{self.lakehouse_name}.{table_name}"):
            self.merge_table(table_name,df, key_columns)
        else:
            self.write_table(table_name,df)

    def merge_table(self,table_name: str,df: DataFrame, key_columns: list):
        delta_table = DeltaTable.forName(self.spark, table_name)
        merge_condition = " AND ".join([f"existing.{col} = updates.{col}" for col in key_columns])
        delta_table.alias("existing").merge(source=df.alias("updates"),condition=merge_condition)\
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def write_table(self, table_name: str,df: DataFrame):
        df.write.format("delta").mode('overwrite').saveAsTable(f"{self.lakehouse_name}.{table_name}")

