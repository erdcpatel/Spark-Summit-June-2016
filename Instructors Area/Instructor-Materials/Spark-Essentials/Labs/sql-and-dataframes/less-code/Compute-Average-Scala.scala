// Databricks notebook source exported at Fri, 22 Jan 2016 00:06:14 UTC
// MAGIC %md
// MAGIC # DataFrames = less code

// COMMAND ----------

val Data = Array(
  "coffee 10",
  "coffee 20",
  "coffee 32",
  "tea 10",
  "tea 15",
  "tea 29"
)

// COMMAND ----------

var rdd = sc.parallelize(Data).map(_.split(" "))


// COMMAND ----------

// MAGIC %md Here it is, not broken down:

// COMMAND ----------

rdd.map { x => (x(0), (x(1).toFloat, 1)) }.
    reduceByKey { case ((num1, count1), (num2, count2)) => 
      (num1 + num2, count1 + count2) 
    }.
    map { case (key, (num, count)) => (key, num / count) }.
    collect()

// COMMAND ----------

// MAGIC %md If you break it down into steps, you can see what's going on.

// COMMAND ----------

val d2 = rdd.map { x => (x(0), (x(1).toFloat, 1)) }
d2.collect()

// COMMAND ----------

val d3 = d2.reduceByKey { case ((num1, count1), (num2, count2)) => (num1 + num2, count1 + count2) }
d3.collect()

// COMMAND ----------

val d4 = d3.map { case (key, (num, count)) => (key, num / count) }
d4.collect()
  

// COMMAND ----------

// MAGIC %md And here's the DataFrame version:

// COMMAND ----------

import org.apache.spark.sql.functions._

val df = rdd.map(a => (a(0), a(1))).toDF("key", "value")
df.groupBy("key")
  .agg(avg("value"))
  .collect()
