from DikwAccelerator.SDL.EMA import *
from DikwAccelerator.CDL.CIA import *
from loguru import logger
from DikwAccelerator.General.Logger import Log
from datetime import datetime
import os
import re
from pyspark.sql import SparkSession


def execute_model(function_name: str, parameters: list, instance: int, spark: SparkSession) -> str:
    try:
        date_dir = datetime.now().strftime("%Y/%m/%d")
        time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        log = Log(
            log_path = f'abfss://DataEngineering@onelake.dfs.fabric.microsoft.com/LH_SFA.Lakehouse/Files/Logs/{function_name}/{date_dir}/{function_name}_{instance}_{time}', ### <<<< add log path here
            spark = spark
        )
        log.start()
        logger.info(f"Start executing model {function_name} for instance {instance}")
        # Build the function call string from the parameters list.
        # Each parameter is converted to its Python literal using repr()
        # so strings will be quoted and numbers/dicts/lists are preserved.
        if parameters is None:
            parameters = []
        elif not isinstance(parameters, (list, tuple)):
            parameters = [parameters]

        args = ", ".join(repr(p) for p in parameters)

        # Always pass the spark session as the last parameter. Use the name
        # 'spark' (not repr) so eval will resolve the actual SparkSession
        # object from the local/global namespace.
        if args:
            args = f"{args}, spark"
        else:
            args = "spark"

        function_call = f"{function_name}({args})"

        # Execute the constructed call in the module's global/local context
        # so functions available in EMA will be resolved.
        result = eval(function_call, globals(), locals())
        logger.info(f"Executed {function_call} -> {result}")

        # add code above
        logger.info(
            f"Finished executing model {function_name} for instance {instance}"
        )
        log.close()

        status = "success"
        return status
    except Exception as e:
        logger.error(
            f"Model execution failed for {function_name} instance {instance}: {e}"
        )
        raise
