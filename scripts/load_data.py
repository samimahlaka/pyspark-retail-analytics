from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Retail_Analytics').getOrCreate()

df = spark.read.csv('data/Retail_Transactions_Dataset.csv', header=True, inferSchema=True
                )
print("Number of rows:", df.count())
df.printSchema()
df.show(5, truncate=False)
