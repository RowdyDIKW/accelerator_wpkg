# DikwAccelerator

**DikwAccelerator** is a Python library providing a suite of utilities for data lakehouse management, data quality assurance, data transformation, and advanced logging. Designed for modern data engineering workflows, it streamlines operations on Spark-based data lakehouses, offering robust tools for schema management, data validation, and seamless transformation between formats like Parquet and XLSX.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage Examples](#usage-examples)
- [Module Overview](#module-overview)
  - [General](#general)
  - [RDL (Relational Data Layer)](#rdl-relational-data-layer)
  - [SDL (Structured Data Layer)](#sdl-structured-data-layer)
- [Configuration & Requirements](#configuration--requirements)
- [Contribution Guidelines](#contribution-guidelines)
- [License](#license)
- [FAQ](#faq)
- [Contact & Support](#contact--support)
- [Changelog](#changelog)
- [References](#references)
- [Architecture Diagram](#architecture-diagram)

---

## Features

- **Lakehouse Utilities**: Manage Spark-based lakehouses, retrieve metadata, and interact with Delta tables.
- **Data Quality**: Deduplication, duplicate checks, and data cleaning for both Spark and pandas DataFrames.
- **Schema Management**: Dataclasses for defining and managing dataset schemas and tables.
- **Transformation**: Convert and load data between Parquet, XLSX, and Delta Lake formats.
- **Logging**: Integrated logging with loguru for robust, file-based logs.
- **Extensible**: Modular design for easy extension and integration.

---

## Installation

Install DikwAccelerator and its dependencies using pip:

```bash
pip install dikw-accelerator
```

**Dependencies:**
- `delta-spark` (install separately if not included)
- `icecream`
- `loguru`
- `pyspark`
- `tqdm`
- `numpy`
- `pandas`

To install all dependencies:

```bash
pip install delta-spark icecream loguru pyspark tqdm numpy pandas
```

---

## Usage Examples

### 1. Initializing Lakehouse Utilities

```python
from DikwAccelerator.General.LakehouseUtils import LakehouseUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
lakehouse = LakehouseUtils(lakehouse_name="my_lakehouse", spark=spark)
```

### 2. Data Quality Utilities

```python
from DikwAccelerator.General.DqUtils import DqUtils

dq = DqUtils()
cleaned_tables = dq.deduplicate_tables(tables_dict)
```

### 3. Schema Management

```python
from DikwAccelerator.General.DataClasses import Schema

schema = Schema(
    name="my_dataset",
    dest="my_lakehouse",
    dataset={"table1": df1, "table2": df2},
    spark=spark,
    key_columns=["ID"]
)
schema.assign_datatypes()
schema.save()
```

### 4. Parquet File Transformation

```python
from DikwAccelerator.SDL.SFA.TransformParquet import TransformParquetFiles

transformer = TransformParquetFiles(
    file_path="/data/parquet/",
    log_path="/logs/",
    schema_name="my_schema",
    dest_lh="my_lakehouse",
    spark=spark
)
transformer.execute()
```

### 5. XLSX File Transformation

```python
from DikwAccelerator.SDL.SFA.TransformXlsx import TransformXlsx

transformer = TransformXlsx(
    file_path="/data/data.xlsx",
    log_path="/logs/",
    schema_name="my_schema",
    dest_lh="my_lakehouse",
    spark=spark
)
transformer.execute()
```

---

## Module Overview

### General

Core utilities and data structures for lakehouse management and data quality.

- **LakehouseUtils**: Manage Spark lakehouses, retrieve metadata, access tables and schemas.
- **DqUtils**: Data quality utilities (deduplication, duplicate checks, config management).
- **Schema**: Dataclass for dataset schema management, saving, and datatype assignment.
- **pandas_clean_old_dates**: Clean old dates in pandas DataFrames.
- **GeneralUtils**: Helper functions for DataFrame operations, type casting, and printing with timestamps.
- **Logger**: Logging utilities using loguru.

### RDL (Relational Data Layer)

Operations and utilities for relational data management.

- **SAA.Executor**: Dataclass for execution context, logging, and Spark integration in relational workflows.

### SDL (Structured Data Layer)

Structured data transformation utilities.

- **SFA.TransformParquetFiles**: Dataclass for transforming Parquet files into Spark DataFrames and Delta tables, with logging.
- **SFA.TransformXlsx**: Dataclass for transforming XLSX files into Spark DataFrames and Delta tables, with logging.

---

## Key Classes and Functions

### General

- **LakehouseUtils**
  - `__init__(lakehouse_name: str, spark: SparkSession)`: Initialize with lakehouse name and Spark session.
  - Methods for retrieving metadata, table names, schemas, and DataFrames.

- **DqUtils**
  - `deduplicate_tables(tables: dict) -> dict`: Deduplicate Spark DataFrames in a dictionary.
  - `duplicates_check(df: DataFrame, key_columns: list)`: Raise error if duplicates found.
  - `get_configs(key: str) -> dict`: Retrieve configuration by key.

- **Schema**
  - `name`, `dest`, `dataset`, `spark`, `key_columns`: Dataset schema definition.
  - `save()`: Save dataset schema to lakehouse.
  - `assign_datatypes()`: Auto-cast datatypes for all tables.

- **pandas_clean_old_dates(df)**
  - Clean dates in pandas DataFrames, setting pre-1900 years to NaN.

### RDL

- **Executor**
  - `instance_id`, `log_path`, `spark`: Execution context for relational data operations.
  - Handles local log directory and timestamping.

### SDL

- **TransformParquetFiles**
  - `file_path`, `log_path`, `schema_name`, `dest_lh`, `spark`: Transformation context.
  - `execute()`: Transform Parquet files and save as Delta tables.

- **TransformXlsx**
  - `file_path`, `log_path`, `schema_name`, `dest_lh`, `spark`: Transformation context.
  - `execute()`: Transform XLSX files and save as Delta tables.

---

## Configuration & Requirements

- **Python 3.7+**
- **Spark environment** (with Delta Lake support)
- **Dependencies**: See [Installation](#installation)
- **Configuration**: Most classes accept parameters for Spark session, file paths, schema names, and lakehouse destinations.

---

## Contribution Guidelines

Contributions are welcome! Please fork the repository and submit a pull request. For major changes, open an issue first to discuss your proposed changes.

- Follow PEP8 style guidelines.
- Write clear docstrings and comments.
- Add tests for new features.

---

## License

[PLACEHOLDER: Specify license here, e.g., MIT License]

---

## FAQ

**Q:** What Spark version is required?  
**A:** Spark 3.x with Delta Lake support is recommended.

**Q:** Can I use this library with cloud-based lakehouses?  
**A:** Yes, as long as you have a compatible Spark session and Delta Lake integration.

**Q:** How do I report bugs or request features?  
**A:** Please open an issue on the GitHub repository.

---

## Contact & Support

For questions, support, or feature requests, please contact [PLACEHOLDER: your-email@example.com] or open an issue on GitHub.

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release notes and version history.

---

## References

- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [loguru](https://github.com/Delgan/loguru)
- [icecream](https://github.com/gruns/icecream)

---

## Architecture Diagram

```mermaid
graph TD
    A[DikwAccelerator]
    A --> B[General]
    A --> C[RDL]
    A --> D[SDL]
    B --> B1[LakehouseUtils]
    B --> B2[DqUtils]
    B --> B3[Schema]
    B --> B4[GeneralUtils]
    B --> B5[Logger]
    C --> C1[SAA.Executor]
    D --> D1[SFA.TransformParquetFiles]
    D --> D2[SFA.TransformXlsx]