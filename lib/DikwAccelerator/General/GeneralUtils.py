import time
from tqdm.notebook import tqdm
from pyspark.sql import SparkSession
import pandas as pd
from loguru import logger

def print_with_current_datetime(message):
    current_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{current_datetime}] {message}")

indent = "-      "

def pandas_to_spark_dfs(dict_of_dfs: dict,dataset_name: str,spark: SparkSession) -> dict:
    try:
        logger.info(f"Transforming pandas dataframes to spark dataframes of dataset: {dataset_name}")
        for key, df in tqdm(dict_of_dfs.items()):
            if isinstance(df, pd.DataFrame):
                logger.info(f"Start transforming {key} to spark dataframe")
                df = df.astype(str)
                dict_of_dfs[key] = spark.createDataFrame(df)
                logger.info(f"Finished transforming {key} to spark dataframe")
        return dict_of_dfs
    except Exception as e:
        logger.error(f"""
        Transforming pandas dataframes to spark dataframes of dataset: {dataset_name} failed
        Failure message: {e}
        """)
        raise


def rename_unnamed_columns(dataframes: dict, dataset_name: str) -> dict:
    try:
        logger.info(f"Start renaming unnamed columns of dataset: {dataset_name}")
        updated_dataframes = {}

        for sheet_name, df in tqdm(dataframes.items()):
            logger.info(f"Start renaming unnamed columns of {sheet_name}")
            updated_df = df.copy()

            unnamed_cols = [col for col in updated_df.columns if "Unnamed" in str(col)]

            for i, col in enumerate(unnamed_cols, start=1):
                new_col_name = f"memo_{i}"
                updated_df.rename(columns={col: new_col_name}, inplace=True)

            updated_dataframes[sheet_name] = updated_df
            logger.info(f"Finished renaming unnamed columns of {sheet_name}")
        logger.info(f"Finished renaming unnamed columns of dataset: {dataset_name}")

        return updated_dataframes
    except Exception as e:
        logger.error(f"renaming unnamed columns of dataset: {dataset_name} failed: {e}")
        raise