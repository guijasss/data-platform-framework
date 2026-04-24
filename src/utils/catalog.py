from typing import Optional
from src.utils.config import SPARK, WATERMARK_COLUMN
from datetime import datetime

def table_exists(table: str) -> bool:
    return SPARK.catalog.tableExists(table)

def get_table_watermark(table: str) -> Optional[datetime]:
    return SPARK.table(table).select(max(WATERMARK_COLUMN)).collect()[0][0]
