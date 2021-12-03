from pyspark.sql import *
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[1]").appName("getoracle").enableHiveSupport().getOrCreate()

#qry = "(select * from emp where sal>2200) test"
#tabs = ["EMP","DEPT","KIDS"]
qry = "(select table_name from tabs) test"
host="jdbc:oracle:thin:@//sanjayoracle.cq8iqbvz0eol.us-east-2.rds.amazonaws.com:1521/ORCL"
mhost="jdbc:mysql://avdmysql.ccyqxxwtivfa.ap-south-1.rds.amazonaws.com:3306/mysqldb"

# Reading data from oracle
tabs = spark.read.format("jdbc").option("url", host).option("driver","oracle.jdbc.OracleDriver").option("user","ouser").option("dbtable",qry).option("password","opassword").load()
print(type(tabs))
print(tabs.show())

# Convert DF to list
row_list = tabs.select(col("table_name")).collect()
list1 = [row.table_name for row in row_list]
print(list1)

#tablist = tabs.select(col("table_name")).rdd.flatMap(lambda x: x).collect()
#tablist = tabs.select(tabs["table_name"]).rdd.map(lambda x:int(x[0])).collect()
#print(tablist)
#df2 = spark.read.format("jdbc").option("url", host).option("driver","oracle.jdbc.OracleDriver").option("user","ouser").option("dbtable",qry).option("password","opassword").load()

for i in list1:
     df = spark.read.format("jdbc").option("url", host).option("driver","oracle.jdbc.OracleDriver").option("user","ouser").option("dbtable",i).option("password","opassword").load()
     #df.write.mode("append").format("jdbc").option("url", mhost).option("driver","com.mysql.cj.jdbc.Driver").option("user","myusername").option("dbtable",i).option("password","mypassword").save()
     #df.show()
     df.write.format("hive").option("path","s3://swapnil-data/output/i").saveAsTable("i")

# Reading data from mysql
#df2 = spark.read.format("jdbc").option("url", mhost).option("driver","com.mysql.cj.jdbc.Driver").option("user","myusername").option("dbtable","dept").option("password","mypassword").load()
#df2.show()