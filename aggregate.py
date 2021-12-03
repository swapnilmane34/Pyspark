from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, avg, round, floor, ceil, udf, sum,upper

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
data = "D:\\Data_Engineer\\Hadoop_Venu\\Dataset\\donations.csv"
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.coalesce(1).write.format("csv").mode("append").save("D:/myresults.csv")
res = df.groupBy(col("name")).agg(avg("amount").alias("avgdon"),sum("amount").alias("totdon"))
#res.show()
res.coalesce(1).write.format("csv").mode("append").save("D:/myresults.csv")
res = df.groupBy(col("name")).agg(avg("amount").alias("avgdon"),sum("amount").alias("totdon"))\
    .withColumn("round",round("avgdon")).withColumn("celi",ceil("avgdon")).withColumn("floor",floor("avgdon"))
#res.show()

# To convert uppercase all column values
res = df.select(*(upper(col(c)).alias(c) for c in df.columns)).show(5)


#round 45.51 .. 46, if 45.49 u ll get 45
#floor  45.51 ... 45, if 45.99 u ll get 45
#ceil 45.51 ... 46, if u 45.01 ull get 46 if 45.99 u ll get 46
#avgmarks .. 55.01... round...56..floor(55),ceil.. 56