// Databricks notebook source exported at Wed, 1 Jun 2016 21:32:45 UTC
// MAGIC %md #![Spark Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark.png)
// MAGIC 
// MAGIC **Objective:**
// MAGIC Provess and analize a live data stream
// MAGIC 
// MAGIC **Time to Complete:**
// MAGIC 15 mins
// MAGIC 
// MAGIC **Data Source:**
// MAGIC Live JSON Stream of Wikipedia edits
// MAGIC 
// MAGIC **Business Questions:**
// MAGIC 
// MAGIC * Question # 1) How many Edits occur every 15 seconds to the English Wikipedia vs. another language?
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC - Connect to a live TCPI/IP stream
// MAGIC - Collect the data into a temp table
// MAGIC - Transform the data so that it suitable for group counts on a per-second basis
// MAGIC - Graph the data
// MAGIC - Show the graph on a dashboard

// COMMAND ----------

// MAGIC %md #### Q-1: How many Edits occur every 15 seconds to the English Wikipedia vs. another language?

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md We start with the batch interval: 15 seconds
// MAGIC * It's short enough that we don't get bored
// MAGIC * It's long enough that we can see something interesting.

// COMMAND ----------

val batchInterval = Seconds(15)

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

baseDSTREAM.foreachRDD { rdd =>
  if(! rdd.isEmpty) {
    sqlContext.read.json(rdd).registerTempTable("all_edits")
  }
}

// COMMAND ----------

// MAGIC %md So that our data is not deleted, configure the `StreamingContext` to remember (or buffer) our data for 1 minute.

// COMMAND ----------

  ssc.remember(Minutes(1))

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
