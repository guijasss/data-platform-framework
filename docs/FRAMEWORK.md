# Aimings and Philosophy
* this framework enforces good practices learnt from years with data engineering work, having trouble with inconsistent data, invisible pipelines, low quality data and unhappy consumers.
* but, definitions like whether to make use of SCD2, what kind of write to do, what fields to use to build a PK and other decisions are up to you to made as a data engineer; you, among everyone in your project, should know your constraints and caveats. This framework only implements abstractions for you to use and speed up development time.
* almost every problem with data platforms is caused by a lack of schema / data contract. This framework ensures every table has a data contract, which should be built even before any pyspark code has been write.
* at least this page will not be wrote using AI, 'cause I want to talk directly to you.

# Constraints
* based on Delta Lake and Medallion Architecture (MA)
* every timestamp will be stored in UTC

# Medalion Architecture Layers

## Bronze
* just like data sources, no transformations.
    * except when column names in source has invalid characters ( ,;{}()\n\t=), which are not acceptable by Delta Lake.
* two columns must always exist: `id` and `processed_at`.
    * `id`: primary key, surrogate key or natural key used to identify each row, therefore, it must be unique.
    * `processed_at`: when row arrived in table (i.e. when that particular row was last inserted or updated).
        * NOTE: if that column is not provided, it will be infered using `current_timestamp()` function. 
* only three accepted write modes
    * APPEND: new records are appended to the table. Useful when data doesn't have a incremental field, but you want to save historical changes.
    * INCREMENTAL: executes a MERGE statement, based on a unique primary key (PK) field, where, if that PK exists, row is updated, otherwise, it's inserted
    * FULL_OVERWRITE: the entire target table is replaced with incoming data.
* {system}_{entity} granularity; e.g. `salesforce_customers` or `hubspot_tickets`.
    * it's up to you to name your systems and entities, but remember, only ONE system-entity pair by table.
* spreadsheet data is often schemaless and not structured, so, I like to put it in a special schema called `external`. In this case, the {system}_{entity} pair is `external_{sheet_name}`.

## Silver
* like bronze layer, should have `id` and `processed_at` columns.
* also, it enforces three write modes described before.
* finally, {system}_{entity} granularity is kept.
* this layer should be cleaned, enriched and transformed as you wish, but NO joins are allowed.

## Gold
* this layer is commonly made using [Go Horse](https://gohorse.com.br/extreme-go-horse-xgh.html) methodology; don't do that! For building gold layer, I propose the following:
    
### dimensional modeling
Maybe you have the “customer” concept spread across multiple source systems (ERPs, databases, etc.), but each one names its properties differently. For example, an ERP might refer to “where the customer lives” as address, while a database might call it location. In this case, you should come up with a common definition for that field. From there, a single, consolidated Customers table can be created in the Gold layer.

This process of identifying entities and standardizing their fields forms the foundation of what is known as Dimensional Modeling. Typically, you will work with two types of tables: facts and dimensions. Facts describe what happened (quantitatively), while dimensions provide context and meaning to those facts (qualitatively).

### feature store

### kpis / metrics / okrs

### model output
