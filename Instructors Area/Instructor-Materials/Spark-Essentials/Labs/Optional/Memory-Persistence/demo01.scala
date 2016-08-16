// Databricks notebook source exported at Wed, 6 Apr 2016 18:38:33 UTC
// MAGIC %md 
// MAGIC # Memory and Persistence Demo
// MAGIC 
// MAGIC This notebook persists an RDD in numerous ways, allowing us to see what happens in the UI when we do.
// MAGIC 
// MAGIC First, let's prepare a small RDD, which we'll cache.

// COMMAND ----------

val path = "dbfs:/mnt/training/philadelphia-crime-data-2015-ytd.csv"
val rdd = sc.textFile(path)

rdd.name = "RDD #1"
rdd.cache()

// COMMAND ----------

// MAGIC %md Let's look at the UI's Storage tab. Is the RDD there?

// COMMAND ----------

// MAGIC %md Let's run an action and look at the UI again.

// COMMAND ----------

rdd.count()

// COMMAND ----------

// MAGIC %md Take note of the:
// MAGIC * Storage Level
// MAGIC * Fraction Cached
// MAGIC * Size in Memory
// MAGIC * Size on Disk

// COMMAND ----------

// MAGIC %md Now let's unpersist it and look at the UI's Storage tab again. Is the RDD still there after we unpersist? Why or why not?

// COMMAND ----------

rdd.unpersist()

// COMMAND ----------

// MAGIC %md Okay, let's change the storage level and see what happens. Let's recache this first RDD, though, so we can compare the two. We'll load a second one and persist it with a different storage level.

// COMMAND ----------

import org.apache.spark.storage.StorageLevel._
rdd.cache()
rdd.count()
val rdd2 = sc.textFile(path)
rdd2.name = "RDD #2"
rdd2.persist(MEMORY_ONLY_SER)
rdd2.count()

// COMMAND ----------

// MAGIC %md 
// MAGIC * What's the Storage tab in the UI look like now?
// MAGIC * What would happen if we unpersisted RDD2 and recached it with `MEMORY_AND_DISK`?

// COMMAND ----------

rdd.unpersist()
rdd2.unpersist()

// COMMAND ----------

// MAGIC %md Now, let's build larger RDD and see what happens when we persist it. 

// COMMAND ----------

val rdd3 = sc.textFile(path)
  .flatMap { line => Array(line, line, line, line, line) }
  .flatMap { line => Array(line, line, line, line, line) }
  .flatMap { line => Array(line, line, line, line, line) }
  .flatMap { line => Array(line, line, line, line, line) }
rdd3.name = "RDD3"
rdd3.cache()
rdd3.take(2)

// COMMAND ----------

// MAGIC %md Depending on your cluster size, you might see a partition cached ... or not. Why? Let's look:

// COMMAND ----------

rdd3.getNumPartitions

// COMMAND ----------

// MAGIC %md Since Spark always caches at the granularity level of a partition, maybe with just 2 partitions, each one is too large to fit in memory.
// MAGIC 
// MAGIC Let's try repartitioning to make the partitions smaller:

// COMMAND ----------

val rdd4 = rdd3.repartition(16)
rdd4.name = "RDD4"
rdd4.cache.take(2)

// COMMAND ----------

// MAGIC %md Now we can see that some data did get cached. Why did just one partition get cached?

// COMMAND ----------

// MAGIC %md Let's unpersist the RDD and re-persist it with a different storage level. If we switch to `MEMORY_ONLY_SER`, how will that affect the amount of data that fits in memory?

// COMMAND ----------

import org.apache.spark.storage.StorageLevel._
rdd4.unpersist()
rdd4.persist(MEMORY_ONLY_SER)
rdd4.take(2)

// COMMAND ----------

// MAGIC %md Now, let's do one more. We'll switch to `MEMORY_AND_DISK` and take one more look at the UI. Again, this will take a few minutes to run.

// COMMAND ----------

rdd4.unpersist()
rdd4.persist(MEMORY_AND_DISK)
rdd4.count()

// COMMAND ----------

// MAGIC %md How would the UI look if we'd used `MEMORY_AND_DISK_2`? What if we'd used `MEMORY_AND_DISK_SER`? Feel free to experiment with those caching strategies.
