// Databricks notebook source exported at Tue, 17 May 2016 21:41:18 UTC
// MAGIC %md ### Solutions to 05_DE-RDD-Datasets RunMe lab

// COMMAND ----------

//Answer to Challenge 1: Why is only one task being used to read this file? If the S3 input split is 64 MB, then why aren't two tasks being used?

// gzip is an unsplittable compression format released in 1992. Therefore to uncompress a gzip file, it has to be read entirely in one machine and uncompressed together. It is not possible to parallelize this, so Spark ends up using just one task to read the file. bzip2, LZO and Snappy are  are examples of splittable compression formats.

// COMMAND ----------

// MAGIC %md **Solutions to Question #2: ** How many requests total did English Wikipedia get in the past hour?

// COMMAND ----------

// Answer to Challenge 2: Can you figure out how to yank out just the requests column and then sum all of the requests?

// Yank out just the requests column
//44 sec
enPagecountsRDD.map(x => x._3).take(5)

// Then build upon that by summing up all of the requests
//51 secs
enPagecountsRDD.map(x => x._3).sum

// COMMAND ----------

// Answer to Challenge 3: Implement this new strategy of collecting the data on the Driver for the summation.

enPagecountsDS.map(x => x._3).collect.sum

// COMMAND ----------

// Answer to Challenge 4: See if you can start with the `enPagecountsDS` Dataset, run a map on it like above, then convert it to a Dataframe and sum the `value` column.

import org.apache.spark.sql.functions._

enPagecountsDS 
  .map(x => x._3)
  .toDF
  .select(sum($"value"))
  .show()
