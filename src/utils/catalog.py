from datetime import datetime
from typing import Optional

from pyspark.sql.functions import max as spark_max

from src.utils.config import WATERMARK_COLUMN, get_spark


def table_exists(table: str) -> bool:
    return get_spark().catalog.tableExists(table)


def get_table_watermark(table: str) -> Optional[datetime]:
    return get_spark().table(table).select(spark_max(WATERMARK_COLUMN)).collect()[0][0]
