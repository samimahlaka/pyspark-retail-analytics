from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum 


spark =  SparkSession.builder.appName('RetaiL Transform').master('local[*]').config('spark.shuffle.partitions',"4").getOrCreate()

df = spark.read.csv('data/Retail_Transactions_Dataset.csv', header=True, inferSchema=True)

clean_df = df.dropna().filter(col('Total_Cost') > 0).cache()

city_revenue = clean_df.groupBy("City").agg(sum("Total_Cost").alias("Revenue")).orderBy(col("Revenue").desc())

print(f'Original Rows : {df.count()}')
print(f'Cleaned Rows : {clean_df.count()}')

print(f'Total Cities : {city_revenue.count()}')
city_revenue.show(10)

clean_df.write.mode('overwrite').parquet('output/retail_transactions_cleaned')

spark.stop()

