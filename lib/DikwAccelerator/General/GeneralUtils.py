import time
from tqdm.notebook import tqdm
from pyspark.sql import SparkSession
import pandas as pd
from icecream import ic
from loguru import logger

def print_with_current_datetime(message):
    current_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{current_datetime}] {message}")

indent = "-      "

def pandas_to_spark_dfs(dict_of_dfs: dict,dataset_name: str,spark: SparkSession, debug=False) -> dict:
    try:
        if not debug:
            ic.disable()
        ic()
        logger.info(f"Transforming pandas dataframes to spark dataframes of dataset: {dataset_name}")
        for key, df in tqdm(dict_of_dfs.items()):
            ic(key)
            if isinstance(df, pd.DataFrame):
                df = df.astype(str)
                dict_of_dfs[key] = spark.createDataFrame(df)
    except Exception as e:
        logger.error(f"""
        Transforming pandas dataframes to spark dataframes of dataset: {dataset_name} failed
        Failure message: {e}
        """)
    return dict_of_dfs


def rename_unnamed_columns(dataframes: dict, dataset_name: str, debug=False) -> dict:
    if not debug:
        ic.disable()
    ic()
    logger.info(f"renaming unnamed columns of dataset: {dataset_name}")
    updated_dataframes = {}

    for sheet_name, df in tqdm(dataframes.items()):
        updated_df = df.copy()

        unnamed_cols = [col for col in updated_df.columns if "Unnamed" in str(col)]

        for i, col in enumerate(unnamed_cols, start=1):
            new_col_name = f"memo_{i}"
            updated_df.rename(columns={col: new_col_name}, inplace=True)

        updated_dataframes[sheet_name] = updated_df

    return updated_dataframes