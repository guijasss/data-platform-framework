from src.utils.config import SPARK
from datetime import datetime

def get_table_watermark(table: str) -> str:
    return 