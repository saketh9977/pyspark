### Data
12-month NSE data is downloaded from [here](https://finance.yahoo.com/quote/%5ENSEI/history/)

### Brief
1. Data is loaded from a CSV into spark dataframe
2. Spark dataframe is converted to a Hive view
3. Hive's SQL interface is used to apply some transformations.
4. Pyspark's UDFs are used to apply custom -
    - single cell transformation
    - multi-col transformation
    - multi-row transformation