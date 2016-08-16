// Databricks notebook source exported at Tue, 17 May 2016 21:41:23 UTC
// MAGIC %md ### Warning: Do not run this notebook! It is for reference only!
// MAGIC 
// MAGIC (Since you don't have write permissions to `/mnt/wikipedia-readonly`, you'll get an error)

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md #### Create the vertices DF:

// COMMAND ----------

val othersDF = sqlContext.createDataFrame(List(
  (1, "other-wikipedia"),
  (2, "other-empty"),
  (3, "other-internal"),
  (4, "other-google"),
  (5, "other-yahoo"),
  (6, "other-bing"),
  (7, "other-facebook"),
  (8, "other-twitter"),
  (9, "other-other")
)).toDF("id", "id_title")

// COMMAND ----------

// MAGIC %md Write the verticesDF to S3 to read in next lab:

// COMMAND ----------

//takes 130 sec to run

val verticesDF = sqlContext.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("delimiter", "\\t")
  .option("mode", "PERMISSIVE")
  .option("inferSchema", "true")
  .load("dbfs:///databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed")
  .select($"curr_id".alias("id"), $"curr_title".alias("id_title"))
  .distinct
  .filter($"id".isNotNull)
  .unionAll(othersDF)
  .repartition(60) // If 100, Each of the partitions will be ~824.0 KB in memory
  .write.parquet("/mnt/wikipedia-readonly/gx_clickstream/vertices") 

// COMMAND ----------

// MAGIC %md #### Create the edges DF:

// COMMAND ----------

// MAGIC %md Write the edgesDF to S3 to read in next lab:

// COMMAND ----------

//takes 180 sec to run

val edgesDF = sqlContext.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("delimiter", "\\t")
  .option("mode", "PERMISSIVE")
  .option("inferSchema", "true")
  .load("dbfs:///databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed")
  .select($"prev_id".alias("src"), $"curr_id".alias("dst"), $"type", $"n", $"prev_title".alias("src_title"), $"curr_title".alias("dst_title"))
  .select($"src", when($"src_title" === "other-wikipedia", 1)
  .when($"src_title" === "other-empty", 2)
  .when($"src_title" === "other-internal", 3)
  .when($"src_title" === "other-google", 4)
  .when($"src_title" === "other-yahoo", 5)
  .when($"src_title" === "other-bing", 6)
  .when($"src_title" === "other-facebook", 7)
  .when($"src_title" === "other-twitter", 8)
  .when($"src_title" === "other-other", 9)
  .otherwise($"src").alias("new_src"), $"dst", $"type", $"n", $"src_title", $"dst_title")
  .drop($"src")
  .withColumnRenamed("new_src", "src")
  .filter($"type" !== "redlink")
  .repartition(60) // If 100, Each of the partitions will be ~11 MB in memory
  .write.parquet("/mnt/wikipedia-readonly/gx_clickstream/edges") 

// COMMAND ----------


