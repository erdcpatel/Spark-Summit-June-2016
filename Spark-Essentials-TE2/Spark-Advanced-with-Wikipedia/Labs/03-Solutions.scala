// Databricks notebook source exported at Wed, 1 Jun 2016 21:23:25 UTC
// MAGIC %md ### Solutions to 03_DA-Pageviews RunMe lab

// COMMAND ----------

// MAGIC %md **Solutions to Question #1:** How many total incoming requests were to the mobile site vs the desktop site?

// COMMAND ----------

// MAGIC %md ** Challenge 1:** Using just the commands we learned so far, can you figure out how to filter the DataFrame for just mobile traffic and then sum the requests column?

// COMMAND ----------

pageviewsDF.filter("site = 'mobile'").select(sum($"requests")).show()

// COMMAND ----------

// MAGIC %md ** Challenge 2:** What about the desktop site? How many requests did it get?

// COMMAND ----------

pageviewsDF.filter($"site" === "desktop").select(sum($"requests")).show()

// COMMAND ----------

// MAGIC %md **Solutions to Question #2:**  What is the start and end range of time for the pageviews data? How many days of data is in the DataFrame?

// COMMAND ----------

// MAGIC %md ** Challenge 3:** Can you figure out how to check which months of 2015 the data covers (using the [Spark API docs](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$))?

// COMMAND ----------

pageviewsTimestampDF.select(month($"timestamp")).distinct().show()

// COMMAND ----------

// MAGIC %md **Solutions to Question #6:** Why is there so much more traffic on Monday vs. other days of the week?

// COMMAND ----------

// MAGIC %md ** Challenge 4:** Can you figure out exactly why there was so much more traffic on Mondays?

// COMMAND ----------

// MAGIC %md **Challenge 4, Hint #1:** - show them that there is a spike on day #110

// COMMAND ----------

// Group the data with dayofyear(..), sum the result, and then order by "Day of year"

val pageviewsByDayOfYearDF = pageviewsTimestampDF.groupBy(dayofyear($"timestamp").alias("Day of year")).sum().orderBy("Day of year")
display(pageviewsByDayOfYearDF)

// You really need to graph the results to "see" what is going on

// COMMAND ----------

// MAGIC %md **Challenge 4, Hint #2:** - view all the data for day 110

// COMMAND ----------

View all the data for day 110, start by adding a collumn for the day-of-year

val pageviewsForDay110 = pageviewsTimestampDF.withColumn("Day of year", dayofyear($"timestamp"))
display(pageviewsForDay110)

// COMMAND ----------

// MAGIC %md **Challenge 4, Hint #3:** - Now filter the data to only see day 110 

// COMMAND ----------

val pageviewsForDay110 = pageviewsTimestampDF.withColumn("Day of year", dayofyear($"timestamp")).where($"Day of year" === 110)
display(pageviewsForDay110)

// COMMAND ----------

// MAGIC %md **Challenge 4, Hint #4:** - Sort the data by timestamp and site

// COMMAND ----------

val pageviewsForDay110 = pageviewsTimestampDF.withColumn("Day of year", dayofyear($"timestamp")).where($"Day of year" === 110).orderBy("timestamp", "site")
display(pageviewsForDay110)
