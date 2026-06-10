from polars import col

from src.reader import ReadConfig, read
from src.writer import WriteConfig, write

read_config = ReadConfig(
    source_table="raw.users",
    method="FULL_LOAD",
    source_watermark_column="created_at"
)

lf = read(read_config).filter(col("id") > 0)

lf.show()

write_config = WriteConfig(
    data=lf,
    method="OVERWRITE",
    target_table="raw.users_refined",
    comments={
        "id": "ID da tabela",
        "name": "Nome do usuário",
        "email": "Email principal do usuário",
        "active": "Indica se está ativo no ERP",
        "created_at": "Data de criação do cadastro"
    }
)

write(write_config)
