from src.reader import read_delta_table
from src.writer import write_delta_table

source_df = read_delta_table("bronze.salesforce_customers")

print("Transform!")

write_delta_table("silver.salesforce_customers", df=source_df, mode="append")
