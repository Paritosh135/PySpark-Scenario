from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
conf = SparkConf().setMaster("local[*]").setAppName("Scenerio-1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# Prepare customers_df
customers_data = [
    (1, "Alice", "Smith", "alice@example.com"),
    (2, "Bob", "Johnson", "bob@example.com"),
    (3, "Charlie", "Lee", "charlie@example.com"),
    (4, "David", "Kim", "david@example.com")
]

customers_columns = ["customer_id", "first_name", "last_name", "email"]
customers_df = spark.createDataFrame(customers_data, customers_columns)
print("######  customers_df  #######")
customers_df.show()


# Prepare orders_df
orders_data = [
    (101, 1, 250.0, "2023-07-01"),
    (102, 2, 150.0, "2023-07-03"),
    (103, 1, 300.0, "2023-07-05"),
    (104, 3, 200.0, "2023-07-07"),
    (105, 1, 120.0, "2023-07-09")
]

orders_columns = ["order_id", "customer_id", "order_amount", "order_date"]
orders_df = spark.createDataFrame(orders_data, orders_columns)
print("######  orders_df  #######")
orders_df.show()


print("\n====== S O L U T I O N ========")

stp1 =customers_df.join(orders_df,["customer_id"],"inner")
print("######  stp1 inner join  #######")
stp1.show()

stp2= ( stp1
        .groupBy("customer_id")
        .agg(count("customer_id").alias("Total_order"))
        .orderBy("customer_id")
)
print("######  stp2 grpby agg  #######")
stp2.show()

stp3= ( stp2
        .join(customers_df,["customer_id"],"left")
        .withColumn("full_name",concat(column('first_name'), column('last_name') ))
        .orderBy("customer_id")
        )
print("######  stp3 left join concat f + l & drop  #######")
stp3.show()

finaldf = stp3.select("customer_id","full_name","email","Total_order")
print("\n### Data processed final output ###")
finaldf.show()
