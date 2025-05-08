import time

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from tqdm.notebook import tqdm

class LakehouseUtils:
    def __init__(self, lakehouse_name: str, spark: SparkSession):
        self.lakehouse_name = lakehouse_name
        self.spark = spark

    def save_tables(self, tables: dict, schema=False, key_columns=False, mode="auto"):
        """
        Save multiple tables to the lakehouse.

        Args:
            tables (dict): Dictionary of {table_name: DataFrame}.
            schema (str or bool): Optional schema name.
            key_columns (list or bool): List of key columns for merge/upsert. If False, overwrite is used.
            mode (str): Write mode, one of:
                - "overwrite": Always overwrite the table.
                - "merge": Upsert using key_columns (SCD1, overwrites changed fields).
                - "scd1": Same as "merge", upsert with overwrite of changed fields (Slowly Changing Dimension Type 1).
                - "scd2": Upsert with history tracking (Slowly Changing Dimension Type 2, requires effective/expiry date columns).
                - "auto" (default): Merge if table exists and key_columns are provided, else overwrite.

        Raises:
            Exception: If saving fails.
        """
        try:
            if schema:
                logger.info(f"Creating schema {schema} if it doesnt exist")
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.lakehouse_name}.{schema}")
            for key, df in tqdm(tables.items(), desc='Saving tables', unit='Table'):
                table_full_name = (f"{schema}." if schema else "") + key
                if key_columns:
                    key_columns_present = [col for col in key_columns if col in df.columns]
                else:
                    key_columns_present = []
                self.save_table(
                    table_full_name,
                    df,
                    key_columns_present,
                    mode=mode
                )
        except Exception as e:
            logger.error(f"save_tables() failed: {e}")
            raise



    def save_table(self, table_name: str, df: DataFrame, key_columns: list, mode="auto"):
        """
        Save a single table to the lakehouse.

        Args:
            table_name (str): Name of the table (optionally schema-qualified).
            df (DataFrame): DataFrame to save.
            key_columns (list): List of key columns for merge/upsert.
            mode (str): Write mode, one of:
                - "overwrite": Always overwrite the table.
                - "merge": Upsert using key_columns (SCD1, overwrites changed fields).
                - "scd1": Same as "merge", upsert with overwrite of changed fields (Slowly Changing Dimension Type 1).
                - "scd2": Upsert with history tracking (Slowly Changing Dimension Type 2, requires effective/expiry date columns).
                - "auto" (default): Merge if table exists and key_columns are provided, else overwrite.

        Raises:
            Exception: If saving fails.
        """
        try:
            table_exists = self.spark.catalog.tableExists(f"{self.lakehouse_name}.{table_name}")
            if mode == "overwrite":
                self.write_table(table_name, df)
            elif mode in ("merge", "scd1"):
                if not key_columns:
                    raise ValueError("key_columns must be provided for merge/scd1 mode.")
                if not table_exists:
                    self.write_table(table_name, df)
                else:
                    self.merge_table(table_name, df, key_columns)
            elif mode == "scd2":
                if not key_columns:
                    raise ValueError("key_columns must be provided for scd2 mode.")
                if not table_exists:
                    self.write_table(table_name, df)
                else:
                    self.merge_table_scd2(table_name, df, key_columns)
            else:  # auto mode (default, backward compatible)
                if table_exists and key_columns:
                    self.merge_table(table_name, df, key_columns)
                else:
                    self.write_table(table_name, df)
        except Exception as e:
            logger.error(f"save_table() failed: {e}")
            raise

    def merge_table(self,table_name: str,df: DataFrame, key_columns: list):
        try:
            logger.info(f"Start merging table: {table_name}")
            delta_table = DeltaTable.forName(self.spark, table_name)
            merge_condition = " AND ".join([f"existing.{col} = updates.{col}" for col in key_columns])
            delta_table.alias("existing").merge(source=df.alias("updates"),condition=merge_condition)\
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            logger.info(f"Finished merging table: {table_name}")
        except Exception as e:
            logger.error(f"merge_table() failed: {e}")
            raise

    def merge_table_scd2(self, table_name: str, df: DataFrame, key_columns: list):
        """
        Perform SCD Type 2 merge: tracks history by expiring old records and inserting new ones.

        The comparison for detecting changed rows excludes 'effective_date' and 'expiry_date' columns,
        focusing only on business keys and non-key attributes. Only new or changed rows are processed:
        - 'effective_date' is set to the current timestamp only for new or changed rows.
        - Unchanged rows are not updated or inserted.
        - The merge condition is robust and prevents unnecessary updates/history records.

        Args:
            table_name (str): Name of the table.
            df (DataFrame): Incoming DataFrame.
            key_columns (list): List of key columns for identifying records.

        Raises:
            Exception: If merge fails.
        """
        from pyspark.sql import functions as F

        logger.info(f"Start SCD2 merge for table: {table_name}")

        delta_table = DeltaTable.forName(self.spark, table_name)
        now = F.current_timestamp()

        # Columns to compare for changes (non-key, non-date columns)
        target_cols = [c for c in df.columns if c not in key_columns + ["effective_date", "expiry_date"]]

        # Read current open records from the target table
        target_df = delta_table.toDF().filter("expiry_date IS NULL")

        # Join input with target on business keys to detect new/changed rows
        join_cond = [df[k] == target_df[k] for k in key_columns]
        joined = df.join(target_df, on=join_cond, how="left")

        # Detect changed rows: any non-key, non-date column differs or target is null (new row)
        change_exprs = [
            (F.col(f"df.{col}") != F.col(f"target_df.{col}")) | F.col(f"target_df.{col}").isNull()
            for col in target_cols
        ]
        is_new = F.col(f"target_df.{key_columns[0]}").isNull()
        is_changed = F.reduce(lambda x, y: x | y, change_exprs) if change_exprs else F.lit(False)
        changed_or_new = is_new | is_changed

        # Only keep new or changed rows
        filtered_df = joined.filter(changed_or_new).select([f"df.{c}" for c in df.columns])

        # Add effective_date and expiry_date columns if missing
        if "effective_date" not in filtered_df.columns:
            filtered_df = filtered_df.withColumn("effective_date", now)
        if "expiry_date" not in filtered_df.columns:
            filtered_df = filtered_df.withColumn("expiry_date", F.lit(None).cast("timestamp"))

        # Build merge condition on key columns and open records (expiry_date is null)
        merge_condition = " AND ".join(
            [f"existing.{col} = updates.{col}" for col in key_columns] +
            ["existing.expiry_date IS NULL"]
        )

        # When matched and data changed, expire the old record
        update_set = {"expiry_date": now}
        # When not matched, insert new record with effective_date=now, expiry_date=null
        insert_set = {col: f"updates.{col}" for col in filtered_df.columns}
        insert_set["effective_date"] = now
        insert_set["expiry_date"] = F.lit(None).cast("timestamp")

        # Build update condition: any non-key, non-date column changed
        update_condition = " OR ".join([f"existing.{col} <> updates.{col}" for col in target_cols])

        delta_table.alias("existing").merge(
            source=filtered_df.alias("updates"),
            condition=merge_condition
        ).whenMatchedUpdate(
            condition=update_condition,
            set=update_set
        ).whenNotMatchedInsert(
            values=insert_set
        ).execute()
        logger.info(f"Finished SCD2 merge for table: {table_name}")

    def write_table(self, table_name: str,df: DataFrame):
        try:
            logger.info(f"Start writing table: {table_name}")
            MAX_RETRIES = 30
            RETRY_DELAY = 3

            for attempt in range(MAX_RETRIES):
                try:
                    df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(f"{self.lakehouse_name}.{table_name}")
                    break
                except Exception as e:
                    if "ConcurrentAppendException" in str(e) and attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                    else:
                        raise
            logger.info(f"Finished writing table: {table_name}")
        except Exception as e:
            logger.error(f"write_table() failed: {e}")
            raise
