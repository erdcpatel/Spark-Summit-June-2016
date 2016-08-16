// Databricks notebook source exported at Tue, 17 May 2016 21:40:37 UTC
// MAGIC %md ### Solutions to 02_DE-Pageviews RunMe lab

// COMMAND ----------

// MAGIC %md **Solution to Question #1:** How many rows in the table refer to mobile vs desktop?

// COMMAND ----------

// MAGIC %md **Challenge 1: ** How many rows refer to desktop?

// COMMAND ----------

pageviewsDF.filter($"site" === "desktop").count

// COMMAND ----------

// MAGIC %md **Challenge 2:** Reload the table from S3, order it by the timestamp and site column (like above) and cache it:
// MAGIC 
// MAGIC Hint: Name the new DataFrame `pageviewsOrderedDF`

// COMMAND ----------

val pageviewsOrderedDF = sqlContext.read.table("pageviews_by_second").orderBy($"timestamp", $"site".desc).cache
