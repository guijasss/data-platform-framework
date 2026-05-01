# Flow
* check if data contract exists and is valid
* enforce the three framework write modes only:
    - `append`
    - `full_overwrite`
    - `incremental`
* validate that requested write mode matches the table data contract
* verify the dataframe contains all columns required by the contract schema
* implement `append` and `full_overwrite` with Delta `saveAsTable`
* implement `incremental` with a Delta `MERGE INTO ... WHEN MATCHED ... WHEN NOT MATCHED ...`
* if the incremental target table does not exist yet, create it with an append write
