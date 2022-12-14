
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, lag

IN_CSV_FILE = '../data/nse-2021-08-to-2022-08.csv'
SQL_FILE = './transformations.sql'

@udf(returnType=IntegerType())
def cell_transformation(cell):
    return cell + 10

@udf(returnType=IntegerType())
def multi_col_transformation(min_col, close_col):
    return close_col - min_col

@udf(returnType=IntegerType())
def multi_row_transformation(cur_row_close, prev_row_close):
    if prev_row_close is None:
        return 0
    return cur_row_close - prev_row_close

def main():
    spark = SparkSession.builder.getOrCreate()
    print('=========================')


    df_raw = spark.read.format('csv').options(header=True).load(IN_CSV_FILE)
    df_subset = df_raw.select('Date', 'Close')
    
    df_subset.createOrReplaceTempView('nse')
    with open(SQL_FILE, 'r') as stream:
        commands = stream.read()
        df_res = spark.sql(commands)

    df_cell_t = df_res.withColumn('close_cell_t', cell_transformation('close'))

    df_multi_col_t = df_cell_t.withColumn('close_min_diff_t', multi_col_transformation('min_', 'close'))

    df_multi_row_t = df_multi_col_t.withColumn('diff_close_t', multi_row_transformation(
        'close', 
        lag('close').over(Window.orderBy('q_date'))
    ))
    df_multi_row_t.show()

if __name__ == '__main__':
    main()