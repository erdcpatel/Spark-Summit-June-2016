// Databricks notebook source exported at Fri, 3 Jun 2016 03:48:14 UTC
// MAGIC %md #![Spark Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark.png)
// MAGIC 
// MAGIC **Objective:**
// MAGIC Process and analyze a live data stream using a sliding window
// MAGIC 
// MAGIC **Time to Complete:**
// MAGIC 5 mins
// MAGIC 
// MAGIC **Data Source:**
// MAGIC Live JSON Stream of Wikipedia edits
// MAGIC 
// MAGIC **Business Questions:**
// MAGIC 
// MAGIC * Question # 1) How many edits occur over a 2 minute window to the English Wikipedia vs. another language?
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC - Process streamed with a 2 minute sliding window

// COMMAND ----------

// MAGIC %md #### Q-1: How many edits occur over a 2 minute window to the English Wikipedia vs. another language?

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md Our final graph will be accurate only to the second so a 1 second batch interval should suffice:

// COMMAND ----------

val batchInterval = Seconds(1)

// COMMAND ----------

// MAGIC %md Next we need the specific port and IP address of the server that is currently streaming the data we want to consume.

// COMMAND ----------

val streamingServerPort = 9002
val streamingServerHost = "54.213.33.240"

// COMMAND ----------

// MAGIC %md In order to create the `StreamingContext`, we first need reference to the `SparkContext`.
// MAGIC 
// MAGIC When running on Databricks, the SparkContext is already created for us...

// COMMAND ----------

sc

// COMMAND ----------

// MAGIC %md Create a new `StreamingContext`, using the SparkContext and batch interval:

// COMMAND ----------

val ssc = new StreamingContext(sc, batchInterval)

// COMMAND ----------

// MAGIC %md Create the DStreams which will include data for all languages:

// COMMAND ----------

val baseDSTREAM = ssc.socketTextStream(streamingServerHost, streamingServerPort)

// COMMAND ----------

// MAGIC %md Register a function to parse the incoming JSON data which in turn register a new "temporary" table every 15 seconds (our batch interval):

// COMMAND ----------

val windowDuration = Minutes(2)
baseDSTREAM.window(windowDuration).transform(_.coalesce(1)).foreachRDD { rdd =>
  if(! rdd.isEmpty) {
    sqlContext.read.json(rdd).registerTempTable("all_edits")
  }
}

// COMMAND ----------

// MAGIC %md You can also run SQL queries on tables defined on streaming data from a different thread (that is, asynchronous to the running StreamingContext). Just make sure that you set the StreamingContext to remember a sufficient amount of streaming data such that the query can run. Otherwise the StreamingContext, which is unaware of the any asynchronous SQL queries, will delete off old streaming data before the query can complete. 
// MAGIC 
// MAGIC For example, if you want to query the last batch, but your query can take 5 minutes to run, then call `streamingContext.remember(Minutes(5))`.

// COMMAND ----------

  ssc.remember(Minutes(5))

// COMMAND ----------

// MAGIC %md Now that's configured, go ahead and start the `StreamingContext`.

// COMMAND ----------

ssc.start

// COMMAND ----------

// MAGIC %md It will take 15 seconds before the first set of data is available, but once it is, we can view the data by selecting from our temp table:

// COMMAND ----------

// MAGIC %sql SELECT * FROM all_edits

// COMMAND ----------

// MAGIC %md Let's narrow down the data to the values we actually care about with a custom select

// COMMAND ----------

// MAGIC %sql SELECT timestamp, channel FROM all_edits order by timestamp, channel

// COMMAND ----------

// MAGIC %md Next we need to clean up our timestamp so that we can group by second not millisecond.

// COMMAND ----------

// MAGIC %sql SELECT substring(cast(timestamp as string), 0, 19) AS time, channel FROM all_edits order by timestamp, channel

// COMMAND ----------

// MAGIC %md Now we can group by the second and channel thus allowing us to get a count:

// COMMAND ----------

// MAGIC %sql SELECT substring(cast(timestamp as string), 0, 19) AS time, channel, count(*) as total FROM all_edits GROUP BY substring(cast(timestamp as string), 0, 19), channel ORDER BY time, channel

// COMMAND ----------

// MAGIC %md Next we can take the graph and move it to a dashboard.
// MAGIC * Keys: **time**
// MAGIC * Series grouping: **channel**
// MAGIC * Values: **total**

// COMMAND ----------

// MAGIC %md And when we are all done, shut down the `StreamingContext`:

// COMMAND ----------

StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
