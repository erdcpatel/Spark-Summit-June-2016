// Databricks notebook source exported at Tue, 17 May 2016 21:41:33 UTC
// MAGIC %md ### Solutions to 05_DE-RDD-Datasets RunMe lab

// COMMAND ----------

// MAGIC %md **Solutions to Question #1: ** How many days of data is in the DataFrame?

// COMMAND ----------

// Challenge 1:  Can you write this query using DataFrames? Hint: Start by importing the sql.functions.


import org.apache.spark.sql.functions._
wikiDF.select(dayofyear($"lastrev_pdt_time")).distinct().show

// COMMAND ----------

// MAGIC %md **Solutions to Question #2: ** How many of the 1 million articles were last edited by [ClueBot NG](https://en.wikipedia.org/wiki/User:ClueBot_NG), an anti-vandalism bot?

// COMMAND ----------

// Answer to Challenge 2
// Write a SQL query to answer this question. The username to search for is `ClueBot BG`.

%sql SELECT COUNT(*) FROM wikipedia WHERE contributorusername = "ClueBot NG";

%sql SELECT * FROM wikipedia WHERE contributorusername = "ClueBot NG" LIMIT 10;
