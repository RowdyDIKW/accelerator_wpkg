import time
from datetime import datetime
from decimal import Decimal

from tqdm.notebook import tqdm
from pyspark.sql import SparkSession, Window
import pandas as pd
from loguru import logger
from pyspark.sql.types import (
    IntegerType, DoubleType, BooleanType, StringType, DateType, NullType, StructField, StructType, TimestampType,
    LongType, DecimalType
)
from pyspark.sql.functions import col, to_date, to_timestamp, trim, row_number
from DikwAccelerator.General.DqUtils import clean_column_names

def print_with_current_datetime(message):
    current_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{current_datetime}] {message}")

indent = "-      "

import json

def flatten_json(pandas_df):
    """
    For any column in the DataFrame that contains arrays (lists) or objects (dicts),
    convert those values to strings (using json.dumps). Other values are left as-is.
    Returns a DataFrame with the same columns, with arrays/objects stringified.
    """
    df = pandas_df.copy()
    for col in df.columns:
        # Check if any value in the column is a list or dict
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
    return df


def pandas_to_spark_dfs(dict_of_dfs: dict,dataset_name: str,spark: SparkSession) -> dict:
    try:
        logger.info(f"Transforming pandas dataframes to spark dataframes of dataset: {dataset_name}")
        for key, df in tqdm(dict_of_dfs.items()):
            completed = False
            try:
                if isinstance(df, pd.DataFrame):
                    logger.info(f"Start transforming {key} to spark dataframe")
                    df = spark.createDataFrame(df)
                    df = convert_nulltype_to_string(df)
                    df = clean_column_names(df)

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
                        df = spark.createDataFrame(df)
                        df = convert_nulltype_to_string(df)
                        df = clean_column_names(df)

                        logger.info(f"Finished transforming {key} to spark dataframe")
                except Exception as e:
                    logger.error(f"pandas_to_spark transformation failed for {key}: {e}")
                    raise
            trimmed_df = df.select(
                [trim(col(column)).alias(column) if dtype == "string" else col(column).alias(column)
                 for column, dtype in df.dtypes]
            )
            dict_of_dfs[key] = trimmed_df

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


def convert_nulltype_to_string(spark_df):
    # Haal het schema van de DataFrame op
    schema = spark_df.schema

    # Maak een nieuw schema waarin NullType wordt vervangen door StringType
    new_schema = StructType([
        StructField(field.name, StringType() if isinstance(field.dataType, NullType) else field.dataType,
                    field.nullable)
        for field in schema
    ])

    # Pas het nieuwe schema toe
    new_spark_df = spark_df.rdd.map(lambda row: row.asDict()).toDF(schema=new_schema)

    return new_spark_df

def add_num_id(df, order_by: str, id_name: str):
    logger.info(f'start adding id column to dataframe')
    window_spec = Window.orderBy(order_by)
    df = df.dropDuplicates().withColumn(id_name, row_number().over(window_spec))
    df = df.select([id_name] + [col for col in df.columns if col != id_name])
    logger.info(f'finished adding id column to dataframe')
    return df


def insert_default_row(df):
    logger.info(f'start inserting default row to dataframe')
    schema = df.schema
    default_values = []
    for field in schema:
        if isinstance(field.dataType, StringType):
            default_values.append("Onbekend")
        elif isinstance(field.dataType, (IntegerType, LongType)):
            default_values.append(-1)
        elif isinstance(field.dataType, DateType):
            default_values.append(datetime(1900, 1, 1).date())
        elif isinstance(field.dataType, TimestampType):
            default_values.append(datetime(1900, 1, 1))
        elif isinstance(field.dataType, DoubleType):
            default_values.append(-1.0)
        elif isinstance(field.dataType, DecimalType):
            default_values.append(Decimal('-1.00'))
        else:
            default_values.append(None)

    default_row = [tuple(default_values)]

    default_df = df.sparkSession.createDataFrame(default_row, schema=schema)
    logger.info(f'finished inserting default row to dataframe')
    return df.union(default_df)