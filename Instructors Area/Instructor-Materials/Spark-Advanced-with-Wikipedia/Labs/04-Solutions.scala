// Databricks notebook source exported at Tue, 17 May 2016 21:29:56 UTC
// MAGIC %md ### Solutions to 04_DA_Clickstream RunMe lab

// COMMAND ----------

// MAGIC %md **Solutions to Question #3:** What are the top 10 articles requested from Wikipedia?

// COMMAND ----------

// Answer to Challenge 1: Can you build upon the code in the cell above to also order by the sum column in descending order, then limit the results to the top ten?

display(clickstreamDF2.groupBy("curr_title").sum().orderBy($"sum(n)".desc).limit(10))

// COMMAND ----------

// MAGIC %md **Solutions to Question #5:** What percentage of the traffic Wikipedia received came from other English Wikipedia pages?

// COMMAND ----------

// Answer to Challenge 2: Can you answer this question using DataFrames? Hint: Filter out all of the rows where the prev_title is google, twitter, facebook, etc and then sum up the n column.

clickstreamDF2
  .filter("prev_title != 'other-google'")
  .filter("prev_title != 'other-twitter'")
  .filter("prev_title != 'other-facebook'")
  .filter("prev_title != 'other-yahoo'")
  .filter("prev_title != 'other-bing'")
  .filter("prev_title != 'other-wikipedia'")
  .filter("prev_title != 'other-empty'")
  .filter("prev_title != 'other-other'")
  .select(sum($"n"))
  .show()

// COMMAND ----------

// MAGIC %md **Solutions to Question #6:** What were the top 5 trending articles on Twitter in Feb 2015?

// COMMAND ----------

// Answer to Challenge 3: Can you answer this question using DataFrames?

display(clickstreamDF2
  .filter("prev_title = 'other-twitter'")
  .groupBy("curr_title")
  .sum()
  .orderBy($"sum(n)".desc)
  .limit(5))

// COMMAND ----------

//Answer to Challenge 4: Try re-writing the query above using SQL:

%sql SELECT curr_title, SUM(n) AS top_twitter FROM clickstream WHERE prev_title = "other-twitter" GROUP BY curr_title ORDER BY top_twitter DESC LIMIT 5;

// COMMAND ----------

// MAGIC  %md **Solutions to Question #9:**
// MAGIC What does the traffic flow pattern look like for the "Apache_Spark" article? 
// MAGIC 
// MAGIC Try writing this query using the DataFrames API:

// COMMAND ----------

//Answer to Challenge 5: Which future articles does the "Apache_Spark" article send most traffic onward to? Try writing this query using the DataFrames API:

display(clickstreamDF2.filter($"prev_title".rlike("""^Apache_Spark$""")).orderBy($"n".desc))

// COMMAND ----------

// MAGIC  %md **Solutions to Instructor Demo:**

// COMMAND ----------

// MAGIC %md **Challenge 6:** Can you figure out why the above cell doesn't complete??

// COMMAND ----------

// Hint: Look at the Spark UI
