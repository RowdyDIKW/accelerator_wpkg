from dataclasses import dataclass
import datetime


@dataclass()
class Executor:
    instance_id : int
    log_path: str
    spark: SparkSession

    def __post_init__(self):
        self.local_log_directory = "/tmp/TransformXlsx_log"
        self.time = f"{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"