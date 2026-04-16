# Constraints
* based on Delta Lake and Medallion Architecture (MA)

# Medalion Architecture Layers
## Bronze
* just like data sources, no transformations
    * except when column names in source has invalid characters ( ,;{}()\n\t=)
* two columns always must exist: `id` and `processed_at`
* only three accepted write modes
    * APPEND: new records are appended to the table. Useful when data doesn't have a incremental field, but you want to save historical changes
    * INCREMENTAL: executes a MERGE statement, based on a unique primary key (PK) field, where, if that PK exists, row is updated, otherwise, it's inserted
    * FULL_OVERWRITE: the entire target table is replaced with incoming data    