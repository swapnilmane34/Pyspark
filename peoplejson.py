from pyspark.sql import *
import os
import pyspark.sql.functions
#import numpy
#import pandas

SparkSession = SparkSession.builder.getOrCreate()
df = SparkSession.read.json("D:\\Data_Engineer\\Hadoop_Venu\\datasets\\people.json")
df.show()
df.printSchema()
df.select("name").show()
df.select(df['name'], df['age'] + 1).show()
df.filter(df['age'] > 21).show()
df.groupBy("age").count().show()

df.createOrReplaceTempView("people")
sqlDF = SparkSession.sql("SELECT * FROM people")
sqlDF.show()

df.createGlobalTempView("people")
SparkSession.sql("SELECT * FROM global_temp.people").show()
SparkSession.newSession().sql("SELECT * FROM global_temp.people").show()

lines = SparkSession.sparkContext.textFile("D:\\Data_Engineer\\Hadoop_Venu\\datasets\\people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1]),location=p[2]))
schemaPeople = SparkSession.createDataFrame(people)
schemaPeople.show()

schemaPeople.createOrReplaceTempView("people")
teenagers = SparkSession.sql("SELECT name FROM people WHERE age >= 15 AND age <= 30")
print(type(teenagers))


teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
for name in teenNames:
    print(name)

# lines = SparkSession.sparkContext.textFile("file:///usr/local/spark/examples/src/main/resources/people.txt")
# parts = lines.map(lambda l: l.split(","))
# people = parts.map(lambda p: (p[0], p[1].strip()))
# schemaString = "name age"
# fields = [pyspark.sql.types.StructField(field_name, pyspark.sql.types.StringType(), True) for field_name in schemaString.split()]
# schema = pyspark.sql.types.StructType(fields)
# schemaPeople = SparkSession.createDataFrame(people, schema)
# schemaPeople.createOrReplaceTempView("people")
# results = SparkSession.sql("SELECT name FROM people")
# results.show()