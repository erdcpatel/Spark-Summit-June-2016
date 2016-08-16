// Databricks notebook source exported at Wed, 6 Apr 2016 21:27:23 UTC
// MAGIC %md
// MAGIC # Spark Streaming and Wikipedia ![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC ## Solutions to Exercises and Questions

// COMMAND ----------

// MAGIC %md
// MAGIC ### Exercise 1
// MAGIC 
// MAGIC _Remove the United States, Canada, Australia, New Zealand and the UK from the data and re-plot the results, to see who's editing English Wikipedia entries from countries where English is not the primary language._

// COMMAND ----------

// MAGIC %md 
// MAGIC There are several possible solutions to this exercise. Here's one:

// COMMAND ----------

display(anonDFCounts.filter(($"country" !== "USA") && ($"country" !== "GBR") && ($"country" !== "CAN") && ($"country" !== "AUS") && ($"country" !== "NZL")))

// COMMAND ----------

// MAGIC %md
// MAGIC But this one's more compact:

// COMMAND ----------

display(anonDFCounts.filter(! ($"country".isin("USA", "CAN", "GBR", "AUS", "NZL"))))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Exercise 2
// MAGIC 
// MAGIC _Choose another language, and figure out where_ that _Wikipedia is being edited. The server currently serves up changes for the following Wikipedias_

// COMMAND ----------

val code = "es" // easy to change in one place
val langDF = cleanedDF.filter(($"wikipedia" === code) && ($"isAnonymous" === true))
                      .select($"user".as("ipAddress"), $"geoCountryCode3".as("country"))
                      .filter(! isnull($"country"))
                      .groupBy($"ipAddress", $"country")
                      .count()
                      .select($"country", $"count")
display(langDF)