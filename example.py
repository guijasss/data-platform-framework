from sqlalchemy import create_engine

from src.reader import read_table
from src.writer import write_table


engine = create_engine("sqlite:///warehouse.db")

source_df = read_table(engine, "bronze_customers")

print("Transform!")

write_table(engine, "silver_customers", df=source_df, mode="append")
