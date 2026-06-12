from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum 


spark =  (SparkSession.builder.appName('RetaiL Transform')
          .master('local[*]')
          .config('spark.sql.shuffle.partitions',"4")
          .getOrCreate())

df = spark.read.csv('data/Retail_Transactions_Dataset.csv', header=True, inferSchema=True)

clean_df = df.dropna().filter(col('Total_Cost') > 0).cache()

city_revenue = (
                clean_df.groupBy("City")
                .agg(sum("Total_Cost").alias("Revenue"))
                .orderBy(col("Revenue").desc())
                )

store_revenue = (
                clean_df.groupBy("Store_Type")
                .agg(sum("Total_Cost").alias("Revenue"))
                .orderBy(col("Revenue").desc())
                )

customer_category_revenue = (
                clean_df.groupBy("Customer_Category")
                .agg(sum("Total_Cost").alias("Revenue"))
                .orderBy(col("Revenue").desc())
                )

payment_revenue = (
                clean_df
                .groupBy("Payment_Method")
                .agg(sum("Total_Cost").alias("Revenue"))
                .orderBy(col("Revenue").desc())
                 )


product_revenue = (
                    clean_df.groupBy("Product")
                    .agg(sum("Total_Cost").alias("Revenue")) 
                    .orderBy(col("Revenue").desc())
                    )



clean_df.createOrReplaceTempView("transactions")

city_revenue_sql = spark.sql(
    """
    SELECT City,SUM(Total_Cost) as revenue_by_city
    FROM transactions
    GROUP BY City
    ORDER BY revenue_by_city DESC
    """
)

top_products_sql = spark.sql(
    """
    SELECT Product, sum(Total_Cost) as revenue_by_product
    From transactions
    Group By Product
    Order by revenue_by_product DESC
    
    """
)

kpi_sql = spark.sql(
    """
    SELECT 
        Count(*) as total_transactions, 
        SUM(Total_Cost) as total_revenue, 
        AVG(Total_Cost) as average_transaction_value
    From transactions
    """)
    


clean_df.write.mode('overwrite').parquet('output/retail_transactions_cleaned')
city_revenue.write.mode("overwrite").parquet("output/analytics/city_revenue")
store_revenue.write.mode("overwrite").parquet("output/analytics/store_revenue")
customer_category_revenue.write.mode("overwrite").parquet("output/analytics/customer_category_revenue")
product_revenue.write.mode("overwrite").parquet("output/analytics/product_revenue")
kpi_sql.write.mode("overwrite").parquet("output/analytics/kpi")

print(f'Total Original Rows : {df.count()}')
print(f'Cleaned Rows : {clean_df.count()}')
print(f'Total Cities : {city_revenue.count()}')
print(f'Total Product Categories : {product_revenue.count()}')
    
city_revenue.show(10)
store_revenue.show(20)
customer_category_revenue.show(20)
product_revenue.show(20)
payment_revenue.show(20)
kpi_sql.show()


print("City Revenue Partitions:", city_revenue.rdd.getNumPartitions())
print("Product Revenue Partitions:", product_revenue.rdd.getNumPartitions())




spark.stop()

