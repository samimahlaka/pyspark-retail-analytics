from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark =  SparkSession.builder.appName('RetaiL Transform').master('local[*]').config('spark.shuffle.partitions',"4"x``).getOrCreate()

df = spark.read.csv('data/Retail_Transactions_Dataset.csv', header=True, inferSchema=True)

clean_df = df.dropna().filter(col('Total_Cost') > 0).cache()

print(f'Original Rows : {df.count()}')
print(f'Cleaned Rows : {clean_df.count()}')

clean_df.write.mode('overwrite').parquet('output/retail_transactions_cleaned')

spark.stop()

