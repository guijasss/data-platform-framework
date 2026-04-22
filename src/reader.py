from datetime import datetime

from src.utils.config import SPARK

def read_delta_table(source_table: str, milestone: datetime) -> None:
    