from DikwAccelerator.RDL.SAA.TransformXlsx import TransformXlsx
from dataclasses import dataclass, field
from loguru import logger
import datetime
import os

@dataclass()
class Executor:
    instance_id : int
    log_path: str
    debug: bool
    spark: SparkSession

    def __post_init__(self):
        self.local_log_directory = "/tmp/TransformXlsx_log"
        self.time = f"{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"