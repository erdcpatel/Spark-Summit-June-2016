# Databricks notebook source exported at Fri, 22 Jan 2016 00:06:05 UTC
# MAGIC %md
# MAGIC # DataFrames = less code

# COMMAND ----------

DATA = ["coffee 10", "coffee 20", "coffee 32", "tea 10", "tea 15", "tea 29"]

# COMMAND ----------

rdd = sc.parallelize(DATA).map(lambda s: s.split())

# COMMAND ----------

# MAGIC %md Here it is, not broken down:

# COMMAND ----------

rdd.map(lambda x: (x[0], (float(x[1]), 1))).\
    reduceByKey(lambda t1, t2: (t1[0] + t2[0], t1[1] + t2[1])).\
    map(lambda t: (t[0], t[1][0] / t[1][1])).\
    collect()

# COMMAND ----------

# MAGIC %md If you break it down into steps, you can see what's going on.

# COMMAND ----------

d2 = rdd.map(lambda x: (x[0], (float(x[1]), 1)))
d2.collect()

# COMMAND ----------

d3 = d2.reduceByKey(lambda t1, t2: (t1[0] + t2[0], t1[1] + t2[1]))
d3.collect()

# COMMAND ----------

d4 = d3.map(lambda t: (t[0], t[1][0] / t[1][1]))
d4.collect()

# COMMAND ----------

# MAGIC %md And here's the DataFrame version.

# COMMAND ----------

from pyspark.sql.functions import *

df = rdd.map(lambda a: (a[0], a[1])).toDF(("key", "value"))
df.groupBy("key").agg(avg("value")).collect()
