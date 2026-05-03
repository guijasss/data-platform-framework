* how to structure transformation pipelines in silver?
    - expose spark dataframe api or encapsulate it somehow?
    - pros and cons?

* data contract model

* it makes sense to implement ci/cd?
    - like, commiting a table code to git would automatically add it to a dag

* how to implement tests?
    - pipeline stress test
        * increasing load, how would pipeline scale?
        * considering execution time, spark cluster resources, etc.
        
* how to structure options?
    - globally options -> config.yaml
        * what options are global?
    - local options -> pyspark code

* what will be project name?

* 4 schemas in "gold" layer
    - dimensional modeling
    - feature store
    - kpis/okrs/
    - models output

* plug 'n play structure
    - support to data catalogs

* use graphqlite to implement table lineage out-of-the-box

* add support to tests
    - compare dataframes
    - code coverage reports
    
* implement migrations