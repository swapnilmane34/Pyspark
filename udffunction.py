from pyspark.sql import *
from pyspark.sql.functions import col,udf

spark = SparkSession.builder.master("local[2]").appName("udffunction").getOrCreate()
data = "D:\\Data_Engineer\\Hadoop_Venu\\datasets\\bank-full.csv"
df = spark.read.format("csv").option("header","true").option("sep",";").option("inferSchema","true").load(data)
#df.show()
df.createOrReplaceTempView("tab")

def offer(*age):
    if age>20 and age<45:
        return "20 % offer"
    elif age >45 and age <60:
        return "40 % offer"
    else:
        return "60 % offer "
age = df.select("age").collect()
tolist = [row.age for row in age]
# conver function to UDF
udffun = udf(offer(tolist))
# Register udf function as sql
spark.udf.register("fun",udffun)

res = spark.sql("select *,fun(age) as weekoffer ")
