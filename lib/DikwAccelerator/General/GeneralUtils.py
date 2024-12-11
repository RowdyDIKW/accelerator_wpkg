import time
from tqdm.notebook import tqdm
from pyspark.sql import SparkSession
import pandas as pd
from loguru import logger
from pyspark.sql.types import (
    IntegerType, DoubleType, BooleanType, StringType, DateType, TimestampType
)
from pyspark.sql.functions import col, to_date, to_timestamp

def print_with_current_datetime(message):
    current_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{current_datetime}] {message}")

indent = "-      "

def pandas_to_spark_dfs(dict_of_dfs: dict,dataset_name: str,spark: SparkSession) -> dict:
    try:
        logger.info(f"Transforming pandas dataframes to spark dataframes of dataset: {dataset_name}")
        for key, df in tqdm(dict_of_dfs.items()):
            completed = False
            try:
                if isinstance(df, pd.DataFrame):
                    logger.info(f"Start transforming {key} to spark dataframe")
                    dict_of_dfs[key] = spark.createDataFrame(df)
                    completed = True
                    logger.info(f"Finished transforming {key} to spark dataframe")
            except:
                pass
            if not completed:
                try:
                    if isinstance(df, pd.DataFrame):
                        logger.info(f"Start transforming {key} to spark dataframe as strings")
                        df = df.astype(str)
                        logger.info(f"auto casting datatypes to {key}")
                        df = auto_cast_dataframe(df)
                        logger.info(f"finished auto casting datatypes to {key}")
                        dict_of_dfs[key] = spark.createDataFrame(df)
                        completed = True
                        logger.info(f"Finished transforming {key} to spark dataframe")
                except Exception as e:
                    logger.error(f"pandas_to_spark transformation failed for {key}: {e}")
                    raise

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

def detect_and_cast_column(df, column):
    """
    Detecteert en cast een enkele kolom naar het meest geschikte datatype.
    """
    # Probeer BooleanType
    try:
        if df.filter((col(column) != "true") & (col(column) != "false") & (col(column).isNotNull())).count() == 0:
            return df.withColumn(column, col(column).cast(BooleanType()))
    except:
        pass

    # Probeer IntegerType
    try:
        if df.filter(col(column).isNotNull() & ~col(column).cast(StringType()).rlike("^[0-9]+$")).count() == 0:
            return df.withColumn(column, col(column).cast(IntegerType()))
    except:
        pass

    # Probeer DoubleType
    try:
        if df.filter(col(column).isNotNull() & ~col(column).cast(StringType()).rlike("^[0-9]+(\\.[0-9]+)?$")).count() == 0:
            return df.withColumn(column, col(column).cast(DoubleType()))
    except:
        pass

    # Probeer DateType
    try:
        test_df = df.withColumn(column, to_date(col(column), "yyyy-MM-dd"))
        if test_df.filter(col(column).isNotNull() & col(column).isNull()).count() == 0:
            return test_df
    except:
        pass

    # Probeer TimestampType
    try:
        test_df = df.withColumn(column, to_timestamp(col(column), "yyyy-MM-dd HH:mm:ss"))
        if test_df.filter(col(column).isNotNull() & col(column).isNull()).count() == 0:
            return test_df
    except:
        pass

    # Als niets werkt, houd StringType
    return df

def auto_cast_dataframe(df):
    """
    Detecteert en cast alle kolommen in een DataFrame naar het meest geschikte datatype.
    """
    for column in df.columns:
        df = detect_and_cast_column(df, column)
    return df