
from pyspark.sql import SparkSession, SQLContext

IN_CSV_FILE = '../data/nse-2021-08-to-2022-08.csv'

def main():
    spark = SparkSession.builder.getOrCreate()
    print('=========================')


    df_raw = spark.read.format('csv').options(header=True).load(IN_CSV_FILE)
    df_subset = df_raw.select('Date', 'Close')
    
    df_subset.createOrReplaceTempView('nse')
    df_res = spark.sql("select count(*) from nse")
    df_res.show()

if __name__ == '__main__':
    main()