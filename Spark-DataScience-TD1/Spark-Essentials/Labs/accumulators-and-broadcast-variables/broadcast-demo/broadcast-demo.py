# Databricks notebook source exported at Mon, 18 Apr 2016 17:11:19 UTC
# MAGIC %md
# MAGIC # Broadcast Variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are Broadcast Variables?
# MAGIC Broadcast Variables allow us to broadcast a read-only copy of non-rdd data to all the executors.  The executors can then access the value of this data locally.  This is much more efficent than relying on the driver to trasmit this data teach time a task is run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using a Broadcast Variable

# COMMAND ----------

# Create a broadcast variable, transmitting it's value to all the executors.
broadcastVar=sc.broadcast([1,2,3])

# I can read it's value
print broadcastVar.value

# COMMAND ----------

# The value is available on the driver
print "Driver:", broadcastVar.value

# And on the executors
mapFunction=lambda n: "Task " + str(n) + ": " + str(broadcastVar.value)
results=sc.parallelize(range(10), numSlices=10).map(mapFunction).collect()
print "\n".join(results)


# COMMAND ----------

# MAGIC %md
# MAGIC ## How Broadcast Variables can improve performance (demo)
# MAGIC Here we have a medium sized dataSet, small enough to fit in RAM, but still involves quite a bit of network communication when sending the dataSet to the executors.

# COMMAND ----------

# Create a medium sized dataSet of several million values.
size=2*1000*1000
dataSet=list(xrange(size))

# Check out the size of the dataSet in RAM.
import sys
print sys.getsizeof(dataSet) / 1000 / 1000, "Megabytes"

# COMMAND ----------

# MAGIC %md Now let's demonstrate the overhead of network communication when not using broadcast variables.

# COMMAND ----------

# Ceate an RDD with 5 partitions so that we can do an operation in 5 seperate tasks running in parallel on up to 5 different executors.
rdd=sc.parallelize([1,2,3,4,5], numSlices=5)
print rdd.getNumPartitions(), "partitions"

# COMMAND ----------

# In a loop, do a job 5 times without using broadcast variables...
for i in range(5):
  rdd.map(lambda x: len(dataSet) * x).collect()

# Look how slow it is...
# This is because our local "data" variable is being used by the lambda and thus must be sent to each executor every time a task is run.

# COMMAND ----------

# MAGIC %md Let's do that again, but this time we'll first send a copy of the dataset to the executors once, so that the data is available locally every time a task is run.

# COMMAND ----------

# Create a broadcast variable.  This will transmit the dataset to the executors.
broadcastVar=sc.broadcast(dataSet)

# COMMAND ----------

# MAGIC %md Now we'll run the job 5 times, and notice how much faster it is since we don't have to retransmit the data set each time.

# COMMAND ----------

for i in range(5):
  rdd.map(lambda x: len(broadcastVar.value)).collect()

# COMMAND ----------

# MAGIC %md Finally, let's delete the the broadcast variable out of the Executor JVMs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Frequently Asked Questions about Broadcast Variables
# MAGIC **Q:** How is this different than using an RDD to keep data on an executor?  
# MAGIC **A:** With an RDD, the data is divided up into partitions and executors hold only a few partitions.  A broadcast variable is sent out to all the executors.
# MAGIC 
# MAGIC **Q:** When should I use an RDD and when should I use a broadcast variable?  
# MAGIC **A:** BroadCast variables must fit into RAM (and they're generally under 20 MB).  And they are on all executors.  They're good for small datasets that you can afford to leave in memory on the executors.  RDDs are better for very large datasets that you want to partition and divide up between executors.

# COMMAND ----------

# MAGIC %md ## How do Broadcasts Work with Dataframes?
# MAGIC 
# MAGIC Broadcasts can be used to improve performance of some kinds of joins when using Dataframes/Dataset/SparkSQL.
# MAGIC 
# MAGIC In many we may want to join one or more (relatively) small tables against a single large dataset -- e.g., "enriching" a transaction or event table (containing, say, customer IDs and store IDs) with additional "business fact tables" (like customer demographic info by ID, and store location and profile by ID). Instead of joining all of these as distributed datasets, typically requiring a shuffle each time, we could broadcast a copy of the small tables to each executor, where they can can be joined directly (through a hash lookup) against the local partitions of the bigger table.
# MAGIC 
# MAGIC This approach is sometimes called a "map-side join" or "hash join" and is related to, but not the same as, "skewed join" in other frameworks.

# COMMAND ----------

# MAGIC %md ### Using Broadcast Joins with Spark
# MAGIC 
# MAGIC By default, Spark will use a shuffle to join two datasets (unless Spark can verify that they are already co-partitioned):

# COMMAND ----------

df1 = sqlContext.range(100)
df2 = sqlContext.range(100)

df1.join(df2, df1["id"] == df2["id"]).collect()

# COMMAND ----------

# MAGIC %md Look at the Spark UI for that job, and note the stage count and the shuffle.
# MAGIC 
# MAGIC To use a broadcast join, we need at least one of the following:
# MAGIC * statistics from running Hive ANALYZE on the table, and the size less than `spark.sql.autoBroadcastJoinThreshold`
# MAGIC * statistics from caching the table in Spark, and the size less than `spark.sql.autoBroadcastJoinThreshold`
# MAGIC * a broadcast hint applied to the table

# COMMAND ----------

from pyspark.sql.functions import broadcast

df1.join(broadcast(df2), df1["id"] == df2["id"]).collect()

# COMMAND ----------

df2.cache().count()
df1.join(df2, df1["id"] == df2["id"]).collect()

# COMMAND ----------

df2.unpersist()
df1.join(df2, df1["id"] == df2["id"]).collect()
