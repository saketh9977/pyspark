### What is this?
A PoC on PySpark, Hive. 
1. 12-month NSE data is downloaded from [here](https://finance.yahoo.com/quote/%5ENSEI/history/)
2. Data is loaded from a CSV into spark dataframe
3. Spark dataframe is converted to a Hive view
4. Hive's SQL interface is used to apply some transformations.
5. Pyspark's UDFs are used to apply custom -
    - single cell transformation
    - multi-col transformation
    - multi-row transformation