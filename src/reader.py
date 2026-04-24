from typing import Dict
from datetime import datetime

from pyspark.sql import DataFrame

from src.utils.config import SPARK, WATERMARK_COLUMN
from src.exceptions import TableDoesNotExistException
from src.protocols import ReadMethod
from src.utils.catalog import table_exists

def __base_read(source_table: str) -> DataFrame:
    return SPARK.table(source_table)


def __incremental_read(source_table: str, milestone: datetime) -> DataFrame:
    return __base_read(source_table).where(f"{WATERMARK_COLUMN} > '{milestone}'")


read_method_mapping: Dict[str, DataFrame] = {
    "incremental": __incremental_read(),
    "full_load": __base_read()
}


def read_delta_table(source_table: str, method: ReadMethod):
    if not table_exists(source_table):
        raise TableDoesNotExistException(source_table)
        
    df: DataFrame = read_method_mapping.get(method.value)
    
    return df
    