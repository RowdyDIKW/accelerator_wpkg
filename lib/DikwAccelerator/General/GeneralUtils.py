import time
from tqdm.notebook import tqdm
from pyspark.sql import SparkSession
import pandas as pd
from icecream import ic

def print_with_current_datetime(message):
    current_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{current_datetime}] {message}")

indent = "-      "

def pandas_to_spark_dfs(dict_of_dfs: dict,spark: SparkSession, debug=False) -> dict:
    if not debug:
        ic.disable()
    ic()
    for key, df in tqdm(dict_of_dfs.items()):
        ic(key)
        if isinstance(df, pd.DataFrame):
            df = df.astype(str)
            dict_of_dfs[key] = spark.createDataFrame(df)
    return dict_of_dfs